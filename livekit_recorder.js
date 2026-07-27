
/**
 * livekit_recorder.js  (v7 — single-FFmpeg A/V sync)
 * =========================================================
 * Connects to a LiveKit room, captures avatar video + audio, and writes
 * rolling MP4 segments (H.264/AAC).
 *
 * KEY DESIGN: A single FFmpeg process receives BOTH video (pipe:3) and
 * audio (pipe:4) simultaneously.  Because both streams share one FFmpeg
 * clock from the very first frame, there is no A/V sync drift.
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
const { spawn } = require('child_process');
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

// ── Token ─────────────────────────────────────────────────────────────────────
async function makeToken(room) {
  const identity = `amplinar-recorder-${Date.now()}`;
  const at = new AccessToken(LK_API_KEY, LK_API_SECRET, { identity, ttl: '4h' });
  at.addGrant({ roomJoin: true, room, canPublish: false, canSubscribe: true, canPublishData: false });
  return at.toJwt();
}

// ── State ─────────────────────────────────────────────────────────────────────
let stopping        = false;
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
// frame triggers FFmpeg start.  Flushed immediately when FFmpeg starts so
// audio and video begin encoding at exactly the same moment.
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
// FFmpeg muxes both on a shared clock and writes directly to an MP4 file.
function startFFmpegSegment(idx, width, height) {
  const outPath = segmentPath(idx);
  console.log(`[lk-rec] Starting FFmpeg segment ${idx}: ${outPath} (${width}x${height})`);

  const proc = spawn('ffmpeg', [
    '-y',
    // Video input on pipe:3
    '-f', 'rawvideo', '-pix_fmt', 'yuv420p',
    '-s', `${width}x${height}`,
    '-r', String(VIDEO_FPS),
    '-thread_queue_size', '512',
    '-i', 'pipe:3',
    // Audio input on pipe:4
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
    // stdin=ignore, stdout=pipe, stderr=pipe, pipe:3=pipe, pipe:4=pipe
    stdio: ['ignore', 'inherit', 'inherit', 'pipe', 'pipe'],
  });

  proc.stdio[3].on('error', (e) => { if (!stopping) console.error('[lk-rec] Video pipe error:', e.message); });
  proc.stdio[4].on('error', (e) => { if (!stopping) console.error('[lk-rec] Audio pipe error:', e.message); });
  proc.on('exit', (code) => console.log(`[lk-rec] FFmpeg seg${idx} exited (code=${code})`));
  proc.on('error', (e) => console.error('[lk-rec] FFmpeg spawn error:', e.message));

  return { proc, outPath };
}

// ── Upload and notify after a segment file is complete ────────────────────────
function finaliseSegment(outPath, idx) {
  const sz = fs.existsSync(outPath) ? fs.statSync(outPath).size : 0;
  console.log(`[lk-rec] Segment ${idx} complete: ${outPath} (${sz} bytes)`);
  if (sz === 0) {
    console.warn(`[lk-rec] Segment ${idx}: empty file — skipping upload`);
    return;
  }
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

            // Flush the tail of the audio prebuffer into FFmpeg.
            // The prebuffer holds audio that arrived before the first video frame.
            // We keep the last ~1.5 seconds (≈72 frames at 48kHz/1024 samples)
            // because those frames correspond to the avatar's first words which
            // start at roughly the same time as the first video frame.
            // Frames older than 1.5s are discarded to avoid audio running ahead.
            const KEEP_FRAMES = 72; // ~1.5 seconds of audio at 48kHz/1024 samples
            if (audioPrebuffer.length > 0) {
              const tail = audioPrebuffer.slice(-KEEP_FRAMES);
              const discarded = audioPrebuffer.length - tail.length;
              console.log(`[lk-rec] Flushing ${tail.length} prebuffer frames into FFmpeg (discarded ${discarded} older frames)`);
              for (const buf of tail) {
                writeAudioFrame(buf);
                audioFrameCount++;
              }
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
    // Close video pipe — FFmpeg will finish encoding when audio pipe also closes
    try { if (videoPipe && !videoPipe.destroyed) videoPipe.end(); } catch (_) {}
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
      while (!stopping) {
        const { done, value: frame } = await reader.read();
        if (done || stopping) break;
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
    // Close audio pipe — FFmpeg will finish encoding once both pipes are closed
    try { if (audioPipe && !audioPipe.destroyed) audioPipe.end(); } catch (_) {}
  })();
}

// ── Graceful stop ─────────────────────────────────────────────────────────────
async function gracefulStop() {
  if (stopping) return;
  stopping = true;

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

  // Upload the final segment
  finaliseSegment(currentOut, segmentIndex);

  // Give the async upload a moment to start before exiting
  await new Promise(r => setTimeout(r, 2000));

  process.exit(0);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[lk-rec] Connecting to ${LK_URL} room=${LK_ROOM}`);
  console.log(`[lk-rec] Output base: ${OUTPUT_PATH}`);
  console.log(`[lk-rec] Segment rolling: ${SEGMENT_MS > 0 ? `every ${SEGMENT_MINUTES} min` : 'disabled'}`);
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
