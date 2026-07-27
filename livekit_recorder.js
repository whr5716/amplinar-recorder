
/**
 * livekit_recorder.js  (v10 — two-point A/V sync trim)
 * =========================================================
 * Connects to a LiveKit room, captures avatar video + audio, and writes
 * rolling MP4 segments (H.264/AAC).
 *
 * KEY DESIGN: A single FFmpeg process receives BOTH video (pipe:3) and
 * audio (pipe:4) simultaneously.  After the raw segment is written, a
 * post-processing step detects the exact lip-movement start (video) and
 * speech start (audio) and trims both streams to align at t=0.  This
 * handles the variable 2-3 second delay between the avatar's lip animation
 * and its TTS audio arriving at the recorder.
 *
 * POST-PROCESSING TRIM (finaliseSegment):
 *   1. silencedetect  → SPEECH_START = first silence_end timestamp
 *   2. scdet on mouth crop (200x80 at x=540,y=370) → LIP_START = first
 *      frame with scene-change score > 0.3
 *   3. Two-point FFmpeg trim: video from LIP_START, audio from SPEECH_START
 *   4. Both streams capped to (video_duration - LIP_START) so audio tail
 *      does not extend past the video end
 *   5. Replaces the raw segment in-place; falls back to original on error
 *
 * Usage:
 *   node livekit_recorder.js <lk_url> <lk_room> <output_path>
 *                            [--segment-minutes=5]
 *                            [--callback-url=http://localhost:8080/segment-complete]
 *                            [--callback-key=<key>]
 *                            [--session-id=<id>]
 *                            [--amplinar-id=<id>]
 *
 * Env vars required:
 *   LIVEKIT_API_KEY    — LiveKit API key
 *   LIVEKIT_API_SECRET — LiveKit API secret
 */

const {
  Room,
  RoomEvent,
  TrackKind,
  VideoStream,
  AudioStream,
  VideoBufferType,
} = require('@livekit/rtc-node');

const { AccessToken } = require('livekit-server-sdk');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { spawn, execFile } = require('child_process');
const fs   = require('fs');
const http = require('http');
const https = require('https');
const path = require('path');

// ── S3 client ─────────────────────────────────────────────────────────────────
const S3_BUCKET = process.env.S3_BUCKET_NAME || 'wholesalehotelrates-images';
const S3_REGION = process.env.S3_REGION      || 'us-east-1';
const s3Client  = new S3Client({
  region: S3_REGION,
  credentials: {
    accessKeyId:     process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
  },
});

async function uploadToS3(filePath, segIdx) {
  const now    = new Date();
  const ts     = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const s3Key  = `amplinar-recordings/${AMPLINAR_ID}/${ts}_${SESSION_ID}_seg${String(segIdx).padStart(3,'0')}.mp4`;
  const body   = fs.readFileSync(filePath);
  const cmd    = new PutObjectCommand({
    Bucket:      S3_BUCKET,
    Key:         s3Key,
    Body:        body,
    ContentType: 'video/mp4',
  });
  await s3Client.send(cmd);
  const url = `https://${S3_BUCKET}.s3.${S3_REGION}.amazonaws.com/${s3Key}`;
  console.log(`[lk-rec] Segment ${segIdx} uploaded to S3: ${url}`);
  return url;
}

// ── Parse args ────────────────────────────────────────────────────────────────
const positional = [];
const flags = {};
for (const arg of process.argv.slice(2)) {
  if (arg.startsWith('--')) {
    const [k, v] = arg.slice(2).split('=');
    flags[k] = v ?? true;
  } else {
    positional.push(arg);
  }
}

const [LK_URL, LK_ROOM, OUTPUT_PATH] = positional;
if (!LK_URL || !LK_ROOM || !OUTPUT_PATH) {
  console.error('[lk-rec] Usage: node livekit_recorder.js <lk_url> <lk_room> <output_path> [--segment-minutes=5] [--callback-url=...] [--callback-key=...] [--session-id=...] [--amplinar-id=...]');
  process.exit(1);
}

const SEGMENT_MINUTES = parseInt(flags['segment-minutes'] || '0', 10);
const CALLBACK_URL    = flags['callback-url'] || '';
const CALLBACK_KEY    = flags['callback-key'] || '';
const SESSION_ID      = flags['session-id']   || '';
const AMPLINAR_ID     = flags['amplinar-id']  || '';

const LK_API_KEY    = process.env.LIVEKIT_API_KEY;
const LK_API_SECRET = process.env.LIVEKIT_API_SECRET;
if (!LK_API_KEY || !LK_API_SECRET) {
  console.error('[lk-rec] LIVEKIT_API_KEY and LIVEKIT_API_SECRET env vars are required');
  process.exit(1);
}

// ── Config ────────────────────────────────────────────────────────────────────
const VIDEO_FPS         = 30;
const AUDIO_SAMPLE_RATE = 48000;
const AUDIO_CHANNELS    = 1;
const SEGMENT_MS        = SEGMENT_MINUTES > 0 ? SEGMENT_MINUTES * 60 * 1000 : 0;

// Mouth crop region for lip-movement detection (1280x720 frame)
// width=200, height=80, x=540, y=370 — covers the avatar's lips
const MOUTH_CROP = 'crop=200:80:540:370';

// Scene-change score threshold for lip detection.
// Idle face: scores 0.1–0.25.  Active lips: scores 0.3+.
const LIP_SCORE_THRESHOLD = 0.3;

// ── Token ─────────────────────────────────────────────────────────────────────
async function makeToken(room) {
  const identity = `amplinar-recorder-${Date.now()}`;
  const at = new AccessToken(LK_API_KEY, LK_API_SECRET, { identity, ttl: '4h' });
  at.addGrant({ roomJoin: true, room, canPublish: false, canSubscribe: true, canPublishData: false });
  return at.toJwt();
}

// ── State ─────────────────────────────────────────────────────────────────────
let stopping            = false;
let videoEnded          = false;   // set when video capture loop exits
let gracefulStopCalled  = false;   // prevents double-entry into gracefulStop
let room            = null;
let videoStream     = null;
let audioStream     = null;
let videoFrameCount = 0;
let audioFrameCount = 0;
let actualWidth     = 1280;
let actualHeight    = 720;

// Current segment state
let segmentIndex    = 0;
let segmentStartMs  = 0;
let segmentRolling  = false;

// Single FFmpeg process — receives video on pipe:3, audio on pipe:4
let ffmpegProc      = null;    // the spawned process
let videoPipe       = null;    // ffmpegProc.stdio[3]
let audioPipe       = null;    // ffmpegProc.stdio[4]
let currentOut      = null;    // output MP4 path for current segment
let ffmpegStarted   = false;   // true once FFmpeg has been spawned

// Audio pre-roll buffer — holds PCM chunks that arrive before the first video
// frame triggers FFmpeg start.  Discarded when FFmpeg starts (pre-video audio
// must not be included — it would push audio ahead of the lips).
let audioPrebuffer  = [];      // Array<Buffer>

// ── Callback to recorder.py ───────────────────────────────────────────────────
function notifySegmentComplete(mergedPath, s3Url, segIdx) {
  if (!CALLBACK_URL) return;
  const body = JSON.stringify({
    session_id:    SESSION_ID,
    amplinar_id:   AMPLINAR_ID,
    segment_index: segIdx,
    video_path:    mergedPath || '',
    audio_path:    s3Url || '',
  });
  const url = new URL(CALLBACK_URL);
  const lib = url.protocol === 'https:' ? https : http;
  const req = lib.request({
    hostname: url.hostname,
    port:     url.port || (url.protocol === 'https:' ? 443 : 80),
    path:     url.pathname,
    method:   'POST',
    headers:  {
      'Content-Type':   'application/json',
      'Content-Length': Buffer.byteLength(body),
      'X-Recorder-Key': CALLBACK_KEY,
    },
  }, (res) => {
    console.log(`[lk-rec] Segment ${segIdx} callback: HTTP ${res.statusCode}`);
  });
  req.on('error', (e) => console.error(`[lk-rec] Segment callback error: ${e.message}`));
  req.write(body);
  req.end();
}

// ── Segment path helper ───────────────────────────────────────────────────────
function segmentPath(idx) {
  const dir  = path.dirname(OUTPUT_PATH);
  const base = path.basename(OUTPUT_PATH, path.extname(OUTPUT_PATH));
  return path.join(dir, `${base}_seg${String(idx).padStart(3,'0')}.mp4`);
}

// ── Start a single FFmpeg process for one segment ─────────────────────────────
// Video arrives on pipe:3 (raw YUV420p), audio arrives on pipe:4 (raw s16le PCM).
// No -itsoffset is used here — the post-processing trim handles sync precisely.
function startFFmpegSegment(idx, width, height) {
  const outPath = segmentPath(idx);
  console.log(`[lk-rec] Starting FFmpeg segment ${idx}: ${outPath} (${width}x${height})`);

  const proc = spawn('ffmpeg', [
    '-y',
    // Video input on pipe:3 — no offset, starts at t=0
    '-f', 'rawvideo', '-pix_fmt', 'yuv420p',
    '-s', `${width}x${height}`,
    '-r', String(VIDEO_FPS),
    '-thread_queue_size', '512',
    '-i', 'pipe:3',
    // Audio input on pipe:4 — no offset; post-processing trim handles sync
    '-f', 's16le', '-ar', String(AUDIO_SAMPLE_RATE), '-ac', String(AUDIO_CHANNELS),
    '-thread_queue_size', '512',
    '-i', 'pipe:4',
    // Video encoding
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
    // Audio encoding
    '-c:a', 'aac', '-b:a', '128k',
    // Output
    '-movflags', '+faststart',
    outPath,
  ], {
    // stdin=ignore, stdout=inherit, stderr=inherit, pipe:3=pipe, pipe:4=pipe
    stdio: ['ignore', 'inherit', 'inherit', 'pipe', 'pipe'],
  });

  proc.stdio[3].on('error', (e) => { if (!stopping) console.error('[lk-rec] Video pipe error:', e.message); });
  proc.stdio[4].on('error', (e) => { if (!stopping) console.error('[lk-rec] Audio pipe error:', e.message); });
  proc.on('exit', (code) => console.log(`[lk-rec] FFmpeg seg${idx} exited (code=${code})`));
  proc.on('error', (e) => console.error('[lk-rec] FFmpeg spawn error:', e.message));

  return { proc, outPath };
}

// ── Run a shell command and return stdout+stderr as a string ──────────────────
function runCommand(cmd, args) {
  return new Promise((resolve, reject) => {
    let out = '';
    const proc = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    proc.stdout.on('data', d => { out += d.toString(); });
    proc.stderr.on('data', d => { out += d.toString(); });
    proc.on('error', reject);
    proc.on('exit', () => resolve(out));
  });
}

// ── Detect speech start time (first silence_end) ──────────────────────────────
async function detectSpeechStart(filePath) {
  try {
    const out = await runCommand('ffmpeg', [
      '-y', '-i', filePath,
      '-af', 'silencedetect=noise=-40dB:duration=0.3',
      '-f', 'null', '-',
    ]);
    // Parse: "silence_end: 8.99137 | silence_duration: ..."
    const match = out.match(/silence_end:\s*([\d.]+)/);
    if (match) {
      const t = parseFloat(match[1]);
      console.log(`[lk-rec] detectSpeechStart: ${t}s`);
      return t;
    }
    console.warn('[lk-rec] detectSpeechStart: no silence_end found — audio may be all speech or all silent');
    return null;
  } catch (e) {
    console.error('[lk-rec] detectSpeechStart error:', e.message);
    return null;
  }
}

// ── Detect lip movement start time (first scdet score > threshold) ────────────
async function detectLipStart(filePath) {
  try {
    const out = await runCommand('ffmpeg', [
      '-y', '-i', filePath,
      '-vf', `${MOUTH_CROP},scdet=threshold=0.1:sc_pass=1`,
      '-f', 'null', '-',
    ]);
    // Parse lines like: lavfi.scd.score: 0.634, lavfi.scd.time: 6.43333
    const lines = out.split('\n');
    for (const line of lines) {
      const m = line.match(/scd\.score:\s*([\d.]+),\s*lavfi\.scd\.time:\s*([\d.]+)/);
      if (m) {
        const score = parseFloat(m[1]);
        const time  = parseFloat(m[2]);
        if (score >= LIP_SCORE_THRESHOLD) {
          console.log(`[lk-rec] detectLipStart: ${time}s (score=${score})`);
          return time;
        }
      }
    }
    console.warn('[lk-rec] detectLipStart: no lip movement detected above threshold');
    return null;
  } catch (e) {
    console.error('[lk-rec] detectLipStart error:', e.message);
    return null;
  }
}

// ── Get video duration via ffprobe ────────────────────────────────────────────
async function getVideoDuration(filePath) {
  try {
    const out = await runCommand('ffprobe', [
      '-v', 'quiet',
      '-show_entries', 'stream=duration',
      '-select_streams', 'v:0',
      '-of', 'csv=p=0',
      filePath,
    ]);
    const dur = parseFloat(out.trim());
    if (!isNaN(dur) && dur > 0) return dur;
    // Fallback: format duration
    const out2 = await runCommand('ffprobe', [
      '-v', 'quiet',
      '-show_entries', 'format=duration',
      '-of', 'csv=p=0',
      filePath,
    ]);
    return parseFloat(out2.trim());
  } catch (e) {
    console.error('[lk-rec] getVideoDuration error:', e.message);
    return null;
  }
}

// ── Two-point sync trim ───────────────────────────────────────────────────────
// Trims video from lipStart and audio from speechStart, joining both at t=0.
// Caps both streams to (videoDuration - lipStart) so audio tail is removed.
// Writes result to a temp file then replaces the original in-place.
async function syncTrimSegment(filePath) {
  console.log(`[lk-rec] syncTrimSegment: starting post-processing on ${filePath}`);

  const speechStart = await detectSpeechStart(filePath);
  const lipStart    = await detectLipStart(filePath);
  const videoDur    = await getVideoDuration(filePath);

  if (speechStart === null || lipStart === null || videoDur === null) {
    console.warn('[lk-rec] syncTrimSegment: detection failed — keeping original file');
    return;
  }

  if (speechStart <= 0 && lipStart <= 0) {
    console.log('[lk-rec] syncTrimSegment: both starts at t=0 — no trim needed');
    return;
  }

  const trimDur = videoDur - lipStart;
  if (trimDur <= 0) {
    console.warn(`[lk-rec] syncTrimSegment: trimDur=${trimDur} <= 0 — skipping`);
    return;
  }

  console.log(`[lk-rec] syncTrimSegment: lipStart=${lipStart}s speechStart=${speechStart}s videoDur=${videoDur}s trimDur=${trimDur}s`);

  const tmpPath = filePath + '.syncing.mp4';
  try {
    const out = await runCommand('ffmpeg', [
      '-y',
      '-ss', String(lipStart),    '-i', filePath,   // video input, seek to lip start
      '-ss', String(speechStart), '-i', filePath,   // audio input, seek to speech start
      '-map', '0:v', '-map', '1:a',
      '-c:v', 'libx264', '-preset', 'fast', '-crf', '23',
      '-c:a', 'aac', '-b:a', '128k',
      '-t', String(trimDur),
      '-shortest',
      '-movflags', '+faststart',
      tmpPath,
    ]);

    if (!fs.existsSync(tmpPath) || fs.statSync(tmpPath).size === 0) {
      console.error('[lk-rec] syncTrimSegment: trim produced empty file — keeping original');
      try { fs.unlinkSync(tmpPath); } catch (_) {}
      return;
    }

    // Verify output durations
    const outDur = await getVideoDuration(tmpPath);
    console.log(`[lk-rec] syncTrimSegment: output duration=${outDur}s (expected ~${trimDur}s)`);

    // Replace original with trimmed version
    fs.renameSync(tmpPath, filePath);
    console.log(`[lk-rec] syncTrimSegment: replaced ${filePath} with synced version`);
  } catch (e) {
    console.error('[lk-rec] syncTrimSegment: trim failed:', e.message);
    try { fs.unlinkSync(tmpPath); } catch (_) {}
  }
}

// ── Upload and notify after a segment file is complete ────────────────────────
async function finaliseSegment(outPath, idx) {
  const sz = fs.existsSync(outPath) ? fs.statSync(outPath).size : 0;
  console.log(`[lk-rec] Segment ${idx} raw complete: ${outPath} (${sz} bytes)`);
  if (sz === 0) {
    console.warn(`[lk-rec] Segment ${idx}: empty file — skipping upload`);
    return;
  }

  // Run two-point sync trim before uploading
  await syncTrimSegment(outPath);

  const sz2 = fs.existsSync(outPath) ? fs.statSync(outPath).size : 0;
  console.log(`[lk-rec] Segment ${idx} post-trim: ${outPath} (${sz2} bytes)`);

  uploadToS3(outPath, idx)
    .then(url => {
      notifySegmentComplete(outPath, url, idx);
      try { fs.unlinkSync(outPath); } catch (_) {}
    })
    .catch(e => {
      console.error(`[lk-rec] S3 upload failed for seg${idx}: ${e.message}`);
      notifySegmentComplete(outPath, '', idx);
    });
}

// ── Close FFmpeg and wait for it to finish writing ────────────────────────────
function closeFFmpegSegment(proc, idx) {
  return new Promise((resolve) => {
    if (!proc) { resolve(); return; }
    // If process already exited, resolve immediately
    if (proc.exitCode !== null || proc.killed) {
      console.log(`[lk-rec] FFmpeg seg${idx} already exited (code=${proc.exitCode})`);
      resolve();
      return;
    }
    const timeout = setTimeout(() => {
      console.warn(`[lk-rec] FFmpeg seg${idx} close timeout — killing`);
      try { proc.kill(); } catch (_) {}
      resolve();
    }, 15000);
    proc.once('exit', () => {
      clearTimeout(timeout);
      resolve();
    });
    // Close both input pipes to signal EOF to FFmpeg
    try { if (proc.stdio[3] && !proc.stdio[3].destroyed) proc.stdio[3].end(); } catch (_) {}
    try { if (proc.stdio[4] && !proc.stdio[4].destroyed) proc.stdio[4].end(); } catch (_) {}
  });
}

// ── Roll to next segment ──────────────────────────────────────────────────────
async function rollSegment() {
  if (segmentRolling) return;
  segmentRolling = true;

  const oldIdx  = segmentIndex;
  const oldProc = ffmpegProc;
  const oldOut  = currentOut;

  // Start new segment immediately so we don't drop frames
  segmentIndex++;
  const { proc, outPath } = startFFmpegSegment(segmentIndex, actualWidth, actualHeight);
  ffmpegProc    = proc;
  videoPipe     = proc.stdio[3];
  audioPipe     = proc.stdio[4];
  currentOut    = outPath;
  segmentStartMs = Date.now();

  console.log(`[lk-rec] Rolled to segment ${segmentIndex}`);

  // Close old segment, then upload
  await closeFFmpegSegment(oldProc, oldIdx);
  finaliseSegment(oldOut, oldIdx);

  segmentRolling = false;
}

// ── Write video frame to FFmpeg ───────────────────────────────────────────────
function writeVideoFrame(yBuf, uBuf, vBuf) {
  if (!videoPipe || videoPipe.destroyed) return;
  try {
    videoPipe.write(yBuf);
    videoPipe.write(uBuf);
    videoPipe.write(vBuf);
  } catch (e) {
    if (!stopping) console.error('[lk-rec] writeVideoFrame error:', e.message);
  }
}

// ── Write audio frame to FFmpeg ───────────────────────────────────────────────
function writeAudioFrame(buf) {
  if (!audioPipe || audioPipe.destroyed) return;
  try {
    audioPipe.write(buf);
  } catch (e) {
    if (!stopping) console.error('[lk-rec] writeAudioFrame error:', e.message);
  }
}

// ── Track capture loops ───────────────────────────────────────────────────────
function startVideoCapture(track) {
  if (videoStream) return;
  console.log('[lk-rec] Starting video capture');
  videoStream = new VideoStream(track);
  const reader = videoStream.getReader();

  (async () => {
    let firstFrame = true;
    try {
      while (!stopping) {
        const { done, value: frameEvent } = await reader.read();
        if (done || stopping) break;
        try {
          const frame = frameEvent.frame;
          const i420  = frame.convert(VideoBufferType.I420);

          if (firstFrame) {
            actualWidth  = i420.width;
            actualHeight = i420.height;
            console.log(`[lk-rec] First video frame: ${actualWidth}x${actualHeight} — starting FFmpeg`);

            // Start single FFmpeg process with both video and audio inputs
            const { proc, outPath } = startFFmpegSegment(0, actualWidth, actualHeight);
            ffmpegProc    = proc;
            videoPipe     = proc.stdio[3];
            audioPipe     = proc.stdio[4];
            currentOut    = outPath;
            segmentStartMs = Date.now();
            ffmpegStarted  = true;

            // Discard the audio prebuffer — pre-video audio must not be included
            // because it would push audio ahead of the lips in the recording.
            if (audioPrebuffer.length > 0) {
              console.log(`[lk-rec] Discarding ${audioPrebuffer.length} pre-video audio frames`);
              audioPrebuffer = [];
            }

            firstFrame = false;
          }

          if (!videoPipe || videoPipe.destroyed) continue;

          const yPlane = i420.getPlane(0);
          const uPlane = i420.getPlane(1);
          const vPlane = i420.getPlane(2);
          if (yPlane && uPlane && vPlane) {
            writeVideoFrame(Buffer.from(yPlane), Buffer.from(uPlane), Buffer.from(vPlane));
            videoFrameCount++;
            if (videoFrameCount % 300 === 0) {
              console.log(`[lk-rec] Video frames: ${videoFrameCount} (${actualWidth}x${actualHeight})`);
            }
          }

          // Roll segment if needed
          if (SEGMENT_MS > 0 && !segmentRolling && (Date.now() - segmentStartMs) >= SEGMENT_MS) {
            rollSegment();
          }
        } catch (e) {
          if (!stopping) console.error('[lk-rec] Video frame error:', e.message);
        }
      }
    } catch (e) {
      if (!stopping) console.error('[lk-rec] Video stream error:', e.message);
    } finally {
      try { reader.releaseLock(); } catch (_) {}
    }
    console.log(`[lk-rec] Video capture ended (${videoFrameCount} frames)`);

    // Signal the audio loop to stop — videoEnded flag is checked by audio loop
    videoEnded = true;
    stopping   = true;

    // Close BOTH pipes to signal EOF to FFmpeg
    try { if (videoPipe && !videoPipe.destroyed) videoPipe.end(); } catch (_) {}
    try { if (audioPipe && !audioPipe.destroyed) audioPipe.end(); } catch (_) {}
    console.log('[lk-rec] Both pipes closed — FFmpeg will finalize');
  })();
}

function startAudioCapture(track) {
  if (audioStream) return;
  console.log('[lk-rec] Starting audio capture');
  audioStream = new AudioStream(track, AUDIO_SAMPLE_RATE, AUDIO_CHANNELS);
  const reader = audioStream.getReader();

  if (!ffmpegStarted) {
    console.log('[lk-rec] Audio track ready — buffering PCM until first video frame starts FFmpeg');
  }

  (async () => {
    try {
      while (!stopping && !videoEnded) {
        const { done, value: frame } = await reader.read();
        if (done || stopping || videoEnded) break;
        try {
          const buf = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
          if (!ffmpegStarted) {
            // FFmpeg not yet started — buffer until video frame triggers it
            audioPrebuffer.push(buf);
            if (audioPrebuffer.length > 3000) audioPrebuffer.shift();
          } else {
            writeAudioFrame(buf);
            audioFrameCount++;
          }
        } catch (e) {
          if (!stopping) console.error('[lk-rec] Audio frame error:', e.message);
        }
      }
    } catch (e) {
      if (!stopping) console.error('[lk-rec] Audio stream error:', e.message);
    } finally {
      try { reader.releaseLock(); } catch (_) {}
    }
    console.log(`[lk-rec] Audio capture ended (${audioFrameCount} frames)`);
    // Audio pipe may already be closed by video capture end — close if still open
    try { if (audioPipe && !audioPipe.destroyed) audioPipe.end(); } catch (_) {}
  })();
}

// ── Graceful stop ─────────────────────────────────────────────────────────────
async function gracefulStop() {
  if (gracefulStopCalled) return;
  gracefulStopCalled = true;
  stopping   = true;
  videoEnded = true;

  console.log('[lk-rec] Stopping capture...');

  // Give reader loops time to drain their last frames
  await new Promise(r => setTimeout(r, 3000));

  // Force-close both pipes to signal EOF to FFmpeg
  try { if (videoPipe && !videoPipe.destroyed) videoPipe.end(); } catch (_) {}
  try { if (audioPipe && !audioPipe.destroyed) audioPipe.end(); } catch (_) {}

  // Wait for FFmpeg to finish writing the file
  await closeFFmpegSegment(ffmpegProc, segmentIndex);

  // Disconnect from room
  if (room) { try { await room.disconnect(); } catch (_) {} }

  console.log(`[lk-rec] Raw data: ${videoFrameCount} video frames, ${audioFrameCount} audio frames`);

  if (videoFrameCount === 0 && audioFrameCount === 0) {
    console.error('[lk-rec] No frames captured');
    process.exit(1);
  }

  // Upload the final segment (includes post-processing trim)
  await finaliseSegment(currentOut, segmentIndex);

  // Give the async upload a moment to start before exiting
  await new Promise(r => setTimeout(r, 2000));

  process.exit(0);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[lk-rec] Connecting to ${LK_URL} room=${LK_ROOM}`);
  console.log(`[lk-rec] Output base: ${OUTPUT_PATH}`);
  console.log(`[lk-rec] Segment rolling: ${SEGMENT_MS > 0 ? `every ${SEGMENT_MINUTES} min` : 'disabled'}`);
  console.log(`[lk-rec] A/V sync: two-point post-processing trim (lip+speech detection)`);
  console.log(`[lk-rec] API key: ${LK_API_KEY}`);

  room = new Room();

  room.on(RoomEvent.TrackSubscribed, (track, publication, participant) => {
    const kindName = track.kind === TrackKind.KIND_AUDIO ? 'AUDIO' : track.kind === TrackKind.KIND_VIDEO ? 'VIDEO' : 'UNKNOWN';
    console.log(`[lk-rec] TrackSubscribed: kind=${kindName} from ${participant.identity} sid=${track.sid}`);
    if (track.kind === TrackKind.KIND_VIDEO && !videoStream) {
      console.log('[lk-rec] → Starting video capture');
      startVideoCapture(track);
    } else if (track.kind === TrackKind.KIND_AUDIO && !audioStream) {
      console.log('[lk-rec] → Starting audio capture');
      startAudioCapture(track);
    }
  });

  room.on(RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
    console.log(`[lk-rec] TrackUnsubscribed: kind=${track.kind} from ${participant.identity}`);
  });

  room.on(RoomEvent.ParticipantConnected, (participant) => {
    console.log(`[lk-rec] ParticipantConnected: ${participant.identity}`);
  });

  room.on(RoomEvent.Disconnected, (reason) => {
    console.log(`[lk-rec] Disconnected: ${reason}`);
    if (!stopping) {
      console.log('[lk-rec] Unexpected disconnect — stopping');
      gracefulStop();
    }
  });

  console.log(`[lk-rec] Generating token for room: ${LK_ROOM}`);
  const LK_TOKEN = await makeToken(LK_ROOM);
  console.log('[lk-rec] Token generated OK');

  await room.connect(LK_URL, LK_TOKEN, { autoSubscribe: true });
  console.log(`[lk-rec] Connected to room: ${room.name}, participants: ${room.remoteParticipants.size}`);

  // Subscribe to any existing unsubscribed tracks
  for (const [, participant] of room.remoteParticipants) {
    console.log(`[lk-rec] Existing participant: ${participant.identity} (${participant.trackPublications.size} tracks)`);
    for (const [, pub] of participant.trackPublications) {
      console.log(`[lk-rec] Existing track: kind=${pub.kind} subscribed=${pub.subscribed} hasTrack=${!!pub.track}`);
      if (!pub.subscribed) {
        try { pub.setSubscribed(true); } catch (e) { console.warn('[lk-rec] setSubscribed error:', e.message); }
      } else if (pub.track) {
        if (pub.track.kind === TrackKind.KIND_VIDEO && !videoStream) {
          console.log(`[lk-rec] Starting video capture for pre-existing track`);
          startVideoCapture(pub.track);
        } else if (pub.track.kind === TrackKind.KIND_AUDIO && !audioStream) {
          console.log(`[lk-rec] Starting audio capture for pre-existing track`);
          startAudioCapture(pub.track);
        }
      }
    }
  }

  // Safety net: after 5 seconds, force-start any missing capture
  setTimeout(() => {
    if (stopping) return;
    for (const [, participant] of room.remoteParticipants) {
      for (const [, pub] of participant.trackPublications) {
        if (pub.track && pub.track.kind === TrackKind.KIND_VIDEO && !videoStream) {
          console.warn(`[lk-rec] SAFETY NET: Force-starting video capture from ${participant.identity}`);
          startVideoCapture(pub.track);
        }
        if (pub.track && pub.track.kind === TrackKind.KIND_AUDIO && !audioStream) {
          console.warn(`[lk-rec] SAFETY NET: Force-starting audio capture from ${participant.identity}`);
          startAudioCapture(pub.track);
        }
      }
    }
    console.log(`[lk-rec] Status after 5s: video=${!!videoStream} audio=${!!audioStream} ffmpegStarted=${ffmpegStarted} videoFrames=${videoFrameCount} audioFrames=${audioFrameCount}`);
  }, 5000);

  // Listen on stdin for commands from recorder.py
  process.stdin.setEncoding('utf8');
  let stdinBuf = '';
  process.stdin.on('data', (chunk) => {
    stdinBuf += chunk;
    let nl;
    while ((nl = stdinBuf.indexOf('\n')) !== -1) {
      const cmd = stdinBuf.slice(0, nl).trim();
      stdinBuf = stdinBuf.slice(nl + 1);
      if (cmd === 'ROLL_SEGMENT') {
        console.log('[lk-rec] Received ROLL_SEGMENT command — rolling segment');
        rollSegment();
      } else if (cmd === 'STOP') {
        console.log('[lk-rec] Received STOP command');
        gracefulStop();
      }
    }
  });
  process.stdin.resume();

  // Wait for stop signal
  await new Promise(resolve => {
    process.once('SIGTERM', () => { console.log('[lk-rec] SIGTERM received'); resolve(); });
    process.once('SIGINT',  () => { console.log('[lk-rec] SIGINT received');  resolve(); });
  });

  await gracefulStop();
}

main().catch(e => {
  console.error('[lk-rec] Fatal:', e);
  try { if (videoPipe && !videoPipe.destroyed) videoPipe.end(); } catch (_) {}
  try { if (audioPipe && !audioPipe.destroyed) audioPipe.end(); } catch (_) {}
  process.exit(1);
});
