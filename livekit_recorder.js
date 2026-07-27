'use strict';

/**
 * livekit_recorder.js  (v6 — MP4 output, rolling segments, crash-safe)
 * =========================================================
 * Connects to a LiveKit room, captures avatar video + audio, and writes
 * rolling MP4 segments (H.264/AAC).  Each segment is:
 *   1. Written via a streaming FFmpeg pipeline (crash-safe — partial file
 *      is always a valid WebM)
 *   2. Uploaded to S3 immediately on completion via a callback to recorder.py
 *
 * If this process crashes, Railway restarts it automatically.  The new
 * instance starts a new segment — all previous segments are already in S3.
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
const { spawn, spawnSync } = require('child_process');
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

const SEGMENT_MINUTES = parseInt(flags['segment-minutes'] || '0', 10);  // 0 = no rolling
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
let videoProc       = null;
let audioProc       = null;
let videoStdin      = null;
let audioStdin      = null;
let currentVideoOut = null;
let currentAudioOut = null;
let segmentRolling  = false;  // true while a roll is in progress

// Audio pre-roll buffer: holds PCM chunks received before the first video frame.
let audioPrebuffer      = [];     // Array<Buffer> — PCM chunks waiting for FFmpeg to start
let ffmpegStarted       = false;  // true once startFFmpegSegment(0,...) has been called

// Sync timestamps — wall-clock ms when the first frame of each type arrived.
// The difference (audioStartMs - videoStartMs) is the audio head-start that
// causes lip-sync offset. We pass it as -itsoffset to FFmpeg at merge time.
let firstAudioMs        = 0;   // Date.now() when first audio frame arrived
let firstVideoMs        = 0;   // Date.now() when first video frame arrived (= FFmpeg start)

// ── Callback to recorder.py ───────────────────────────────────────────────────
function notifySegmentComplete(videoPath, audioPath, segIdx) {
  if (!CALLBACK_URL) return;
  const body = JSON.stringify({
    session_id:   SESSION_ID,
    amplinar_id:  AMPLINAR_ID,
    segment_index: segIdx,
    video_path:   videoPath || '',
    audio_path:   audioPath || '',
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

// ── FFmpeg segment management ─────────────────────────────────────────────────
function segmentPath(idx, suffix) {
  const dir  = path.dirname(OUTPUT_PATH);
  const base = path.basename(OUTPUT_PATH, path.extname(OUTPUT_PATH));
  return path.join(dir, `${base}_seg${String(idx).padStart(3,'0')}${suffix}`);
}

function startFFmpegSegment(idx, width, height) {
  const vOut = segmentPath(idx, '_video.mp4');
  const aOut = segmentPath(idx, '_audio.aac');

  console.log(`[lk-rec] Starting FFmpeg segment ${idx}: ${vOut}`);

  // H.264 video — ultrafast preset for real-time encoding
  const vProc = spawn('ffmpeg', [
    '-y',
    '-f', 'rawvideo', '-pix_fmt', 'yuv420p',
    '-s', `${width}x${height}`,
    '-r', String(VIDEO_FPS),
    '-i', 'pipe:0',
    '-c:v', 'libx264', '-preset', 'ultrafast', '-crf', '23',
    '-movflags', '+frag_keyframe+empty_moov+default_base_moof',
    '-an', vOut,
  ], { stdio: ['pipe', 'inherit', 'inherit'] });

  vProc.on('exit', (code) => console.log(`[lk-rec] FFmpeg video seg${idx} exited (code=${code})`));

  // AAC audio
  const aProc = spawn('ffmpeg', [
    '-y',
    '-f', 's16le', '-ar', String(AUDIO_SAMPLE_RATE), '-ac', String(AUDIO_CHANNELS),
    '-i', 'pipe:0',
    '-c:a', 'aac', '-b:a', '128k',
    aOut,
  ], { stdio: ['pipe', 'inherit', 'inherit'] });

  aProc.on('exit', (code) => console.log(`[lk-rec] FFmpeg audio seg${idx} exited (code=${code})`));

  return { vProc, aProc, vOut, aOut };
}

function closeFFmpegSegment(vProc, aProc, vOut, aOut, idx) {
  return new Promise((resolve) => {
    let vDone = false, aDone = false;
    const check = () => { if (vDone && aDone) resolve(); };

    if (vProc && !vProc.stdin.destroyed) {
      vProc.stdin.end();
    }
    if (aProc && !aProc.stdin.destroyed) {
      aProc.stdin.end();
    }

    const timeout = setTimeout(() => {
      console.warn(`[lk-rec] FFmpeg seg${idx} close timeout — killing`);
      try { vProc && vProc.kill(); } catch (_) {}
      try { aProc && aProc.kill(); } catch (_) {}
      resolve();
    }, 10000);

    if (vProc) {
      vProc.once('exit', () => { vDone = true; check(); });
    } else { vDone = true; }

    if (aProc) {
      aProc.once('exit', () => { aDone = true; check(); });
    } else { aDone = true; }

    check();
    // Clear timeout once resolved
    const origResolve = resolve;
    resolve = (...args) => { clearTimeout(timeout); origResolve(...args); };
  });
}

// ── Roll to next segment ──────────────────────────────────────────────────────
async function rollSegment() {
  if (segmentRolling) return;
  segmentRolling = true;

  const oldIdx      = segmentIndex;
  const oldVProc    = videoProc;
  const oldAProc    = audioProc;
  const oldVOut     = currentVideoOut;
  const oldAOut     = currentAudioOut;

  // Start new segment immediately so we don't drop frames
  segmentIndex++;
  const { vProc, aProc, vOut, aOut } = startFFmpegSegment(segmentIndex, actualWidth, actualHeight);
  videoProc       = vProc;
  audioProc       = aProc;
  videoStdin      = vProc.stdin;
  audioStdin      = aProc.stdin;
  currentVideoOut = vOut;
  currentAudioOut = aOut;
  segmentStartMs  = Date.now();

  console.log(`[lk-rec] Rolled to segment ${segmentIndex}`);

  // Close old segment and notify callback
  await closeFFmpegSegment(oldVProc, oldAProc, oldVOut, oldAOut, oldIdx);

  // Merge old segment and notify
  const mergedOut = segmentPath(oldIdx, '.mp4');
  mergeSegment(oldVOut, oldAOut, mergedOut, oldIdx);

  segmentRolling = false;
}

function mergeSegment(vOut, aOut, mergedOut, idx) {
  const vExists = vOut && fs.existsSync(vOut) && fs.statSync(vOut).size > 0;
  const aExists = aOut && fs.existsSync(aOut) && fs.statSync(aOut).size > 0;

  let args;
  if (vExists && aExists) {
    // Calculate audio head-start: how many seconds of audio arrived before the first video frame.
    // If audio started before video (firstAudioMs < firstVideoMs), the audio file has extra
    // content at the start that needs to be trimmed. We use -itsoffset on the audio input
    // to tell FFmpeg to skip that many seconds from the start of the audio stream.
    const audioHeadStartSec = (firstVideoMs > 0 && firstAudioMs > 0 && firstAudioMs < firstVideoMs)
      ? (firstVideoMs - firstAudioMs) / 1000
      : 0;
    console.log(`[lk-rec] Seg${idx} merge: audioHeadStart=${audioHeadStartSec.toFixed(3)}s (audioMs=${firstAudioMs}, videoMs=${firstVideoMs})`);
    if (audioHeadStartSec > 0.05) {
      // Trim the audio head-start by seeking into the audio file before muxing
      args = ['-y',
              '-i', vOut,
              '-ss', audioHeadStartSec.toFixed(6), '-i', aOut,
              '-c:v', 'copy', '-c:a', 'copy',
              '-movflags', '+faststart', mergedOut];
    } else {
      // No meaningful offset — mux directly
      args = ['-y', '-i', vOut, '-i', aOut, '-c:v', 'copy', '-c:a', 'copy',
              '-movflags', '+faststart', mergedOut];
    }
  } else if (vExists) {
    args = ['-y', '-i', vOut, '-c', 'copy', '-movflags', '+faststart', mergedOut];
  } else if (aExists) {
    args = ['-y', '-i', aOut, '-c', 'copy', mergedOut];
  } else {
    console.warn(`[lk-rec] Segment ${idx}: no video or audio output — skipping`);
    return;
  }

  const result = spawnSync('ffmpeg', args, { stdio: 'inherit', timeout: 60000 });
  if (result.status !== 0) {
    console.error(`[lk-rec] Segment ${idx} merge failed (code=${result.status})`);
    // Fall back to video-only if merge failed
    if (vExists) {
      try { fs.copyFileSync(vOut, mergedOut); } catch (_) {}
    }
  }

  // Clean up intermediate files
  try { if (vOut) fs.unlinkSync(vOut); } catch (_) {}
  try { if (aOut) fs.unlinkSync(aOut); } catch (_) {}

  const sz = fs.existsSync(mergedOut) ? fs.statSync(mergedOut).size : 0;
  console.log(`[lk-rec] Segment ${idx} merged: ${mergedOut} (${sz} bytes)`);

  if (sz > 0) {
    // Upload directly to S3 from Node.js — does NOT depend on recorder.py being alive
    uploadToS3(mergedOut, idx)
      .then(url => {
        // Also notify recorder.py so it can store the URL in the DB
        notifySegmentComplete(mergedOut, url, idx);
        // Clean up local file after successful upload
        try { fs.unlinkSync(mergedOut); } catch (_) {}
      })
      .catch(e => {
        console.error(`[lk-rec] S3 upload failed for seg${idx}: ${e.message}`);
        // Still notify recorder.py with local path as fallback
        notifySegmentComplete(mergedOut, '', idx);
      });
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
            firstVideoMs = Date.now();
            console.log(`[lk-rec] First video frame: ${actualWidth}x${actualHeight} at t=${firstVideoMs}`);
            const { vProc, aProc, vOut, aOut } = startFFmpegSegment(0, actualWidth, actualHeight);
            videoProc       = vProc;
            audioProc       = aProc;
            videoStdin      = vProc.stdin;
            audioStdin      = aProc.stdin;
            currentVideoOut = vOut;
            currentAudioOut = aOut;
            segmentStartMs  = firstVideoMs;
            ffmpegStarted   = true;
            // Discard any audio buffered before the first video frame — those frames
            // predate the video and would cause audio to run ahead of the lips.
            if (audioPrebuffer.length > 0) {
              console.log(`[lk-rec] Discarding ${audioPrebuffer.length} pre-video audio frames`);
              audioPrebuffer = [];
            }
            firstFrame = false;
          }

          if (!videoStdin || videoStdin.destroyed) continue;

          const yPlane = i420.getPlane(0);
          const uPlane = i420.getPlane(1);
          const vPlane = i420.getPlane(2);
          if (yPlane && uPlane && vPlane) {
            videoStdin.write(Buffer.from(yPlane));
            videoStdin.write(Buffer.from(uPlane));
            videoStdin.write(Buffer.from(vPlane));
            videoFrameCount++;
            if (videoFrameCount % 300 === 0) {
              console.log(`[lk-rec] Video frames: ${videoFrameCount} (${actualWidth}x${actualHeight})`);
            }
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
    if (videoStdin && !videoStdin.destroyed) { try { videoStdin.end(); } catch (_) {} }
  })();
}

function startAudioCapture(track) {
  if (audioStream) return;
  console.log('[lk-rec] Starting audio capture');
  audioStream = new AudioStream(track, AUDIO_SAMPLE_RATE, AUDIO_CHANNELS);
  const reader = audioStream.getReader();

  // NOTE: Do NOT start FFmpeg here even if audioStdin is null.
  // Audio frames are buffered in audioPrebuffer until the first video frame arrives
  // and triggers startFFmpegSegment(). This guarantees A/V sync: both streams start
  // encoding from the exact same moment (the first video frame).
  if (!ffmpegStarted) {
    console.log('[lk-rec] Audio track ready — buffering PCM until first video frame (A/V sync)');
  }

  (async () => {
    try {
      while (!stopping) {
        const { done, value: frame } = await reader.read();
        if (done || stopping) break;
        try {
          const buf = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
          if (!ffmpegStarted) {
            // FFmpeg not yet started — buffer this PCM chunk and record when audio first arrived
            if (firstAudioMs === 0) {
              firstAudioMs = Date.now();
              console.log(`[lk-rec] First audio frame at t=${firstAudioMs}`);
            }
            audioPrebuffer.push(buf);
            if (audioPrebuffer.length > 3000) audioPrebuffer.shift();
          } else {
            // FFmpeg is running — write directly
            if (audioStdin && !audioStdin.destroyed) {
              audioStdin.write(buf);
              audioFrameCount++;
            }
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
    if (audioStdin && !audioStdin.destroyed) { try { audioStdin.end(); } catch (_) {} }
  })();
}

// ── Graceful stop ─────────────────────────────────────────────────────────────
async function gracefulStop() {
  if (stopping) return;
  stopping = true;

  console.log('[lk-rec] Stopping capture...');

  // Signal reader loops to exit — they check stopping at each iteration
  // and will call videoStdin.end() / audioStdin.end() when they exit.
  // Wait for them to drain.
  await new Promise(r => setTimeout(r, 3000));

  // Force-close stdin pipes (belt-and-suspenders)
  if (videoStdin && !videoStdin.destroyed) { try { videoStdin.end(); } catch (_) {} }
  if (audioStdin && !audioStdin.destroyed) { try { audioStdin.end(); } catch (_) {} }

  // Wait for FFmpeg to finalise the current segment
  await new Promise(r => setTimeout(r, 5000));

  // Disconnect from room
  if (room) { try { await room.disconnect(); } catch (_) {} }

  console.log(`[lk-rec] Raw data: ${videoFrameCount} video frames, ${audioFrameCount} audio frames`);

  if (videoFrameCount === 0 && audioFrameCount === 0) {
    console.error('[lk-rec] No frames captured');
    process.exit(1);
  }

  // Merge and notify the final segment
  const finalMerged = segmentPath(segmentIndex, '.mp4');
  mergeSegment(currentVideoOut, currentAudioOut, finalMerged, segmentIndex);

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
    console.log(`[lk-rec] TrackSubscribed: kind=${track.kind} (${track.kind === TrackKind.KIND_AUDIO ? 'AUDIO' : track.kind === TrackKind.KIND_VIDEO ? 'VIDEO' : 'UNKNOWN'}) from ${participant.identity} sid=${track.sid}`);
    if (track.kind === TrackKind.KIND_VIDEO && !videoStream) {
      console.log('[lk-rec] → Starting video capture from TrackSubscribed');
      startVideoCapture(track);
    } else if (track.kind === TrackKind.KIND_AUDIO && !audioStream) {
      console.log('[lk-rec] → Starting audio capture from TrackSubscribed');
      startAudioCapture(track);
    } else {
      console.log(`[lk-rec] → Skipping track: videoStream=${!!videoStream} audioStream=${!!audioStream}`);
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
  console.log(`[lk-rec] autoSubscribe=true — TrackSubscribed events will fire for all tracks`);

  // Subscribe to any existing unsubscribed tracks, and start capture for already-subscribed ones
  // (TrackSubscribed events for pre-existing tracks may have fired before our handler was registered)
  console.log(`[lk-rec] Scanning ${room.remoteParticipants.size} existing participant(s) for tracks...`);
  for (const [, participant] of room.remoteParticipants) {
    console.log(`[lk-rec] Existing participant: ${participant.identity} (${participant.trackPublications.size} tracks)`);
    for (const [, pub] of participant.trackPublications) {
      console.log(`[lk-rec] Existing track: kind=${pub.kind} subscribed=${pub.subscribed} muted=${pub.muted} hasTrack=${!!pub.track} sid=${pub.sid}`);
      if (!pub.subscribed) {
        try { pub.setSubscribed(true); console.log(`[lk-rec] Called setSubscribed(true) on ${pub.sid}`); }
        catch (e) { console.warn('[lk-rec] setSubscribed error:', e.message); }
      } else if (pub.track) {
        // Already subscribed — start capture if not already started (may have been missed)
        if (pub.track.kind === TrackKind.KIND_VIDEO && !videoStream) {
          console.log(`[lk-rec] Starting video capture for pre-existing track from ${participant.identity}`);
          startVideoCapture(pub.track);
        } else if (pub.track.kind === TrackKind.KIND_AUDIO && !audioStream) {
          console.log(`[lk-rec] Starting audio capture for pre-existing track from ${participant.identity}`);
          startAudioCapture(pub.track);
        }
      }
    }
  }

  // Safety net: after 5 seconds, if audio capture still hasn't started, force-start it
  // from any available pre-existing audio track. This handles the case where TrackSubscribed
  // fires asynchronously after our initial scan and we need to wait for it.
  setTimeout(() => {
    if (stopping) return;
    if (!audioStream) {
      console.warn('[lk-rec] SAFETY NET: Audio capture not started after 5s — scanning for audio track...');
      for (const [, participant] of room.remoteParticipants) {
        for (const [, pub] of participant.trackPublications) {
          if (pub.track && pub.track.kind === TrackKind.KIND_AUDIO && !audioStream) {
            console.warn(`[lk-rec] SAFETY NET: Force-starting audio capture from ${participant.identity}`);
            startAudioCapture(pub.track);
          }
        }
      }
      if (!audioStream) {
        console.warn('[lk-rec] SAFETY NET: No audio track found after 5s — audio will be missing from recording');
      }
    } else {
      console.log(`[lk-rec] Audio capture confirmed running after 5s (${audioFrameCount} frames so far)`);
    }
    if (!videoStream) {
      console.warn('[lk-rec] SAFETY NET: Video capture not started after 5s — scanning for video track...');
      for (const [, participant] of room.remoteParticipants) {
        for (const [, pub] of participant.trackPublications) {
          if (pub.track && pub.track.kind === TrackKind.KIND_VIDEO && !videoStream) {
            console.warn(`[lk-rec] SAFETY NET: Force-starting video capture from ${participant.identity}`);
            startVideoCapture(pub.track);
          }
        }
      }
    }
  }, 5000);

  // Listen on stdin for commands from recorder.py
  // Commands: ROLL_SEGMENT\n
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
  if (videoStdin && !videoStdin.destroyed) { try { videoStdin.end(); } catch (_) {} }
  if (audioStdin && !audioStdin.destroyed) { try { audioStdin.end(); } catch (_) {} }
  process.exit(1);
});
