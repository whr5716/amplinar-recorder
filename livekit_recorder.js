'use strict';

/**
 * livekit_recorder.js  (v5 — rolling segments, crash-safe)
 * =========================================================
 * Connects to a LiveKit room, captures avatar video + audio, and writes
 * rolling WebM segments.  Each segment is:
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
  const s3Key  = `amplinar-recordings/${AMPLINAR_ID}/${ts}_${SESSION_ID}_seg${String(segIdx).padStart(3,'0')}.webm`;
  const body   = fs.readFileSync(filePath);
  const cmd    = new PutObjectCommand({
    Bucket:      S3_BUCKET,
    Key:         s3Key,
    Body:        body,
    ContentType: 'video/webm',
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
  const vOut = segmentPath(idx, '_video.webm');
  const aOut = segmentPath(idx, '_audio.webm');

  console.log(`[lk-rec] Starting FFmpeg segment ${idx}: ${vOut}`);

  const vProc = spawn('ffmpeg', [
    '-y',
    '-f', 'rawvideo', '-pix_fmt', 'yuv420p',
    '-s', `${width}x${height}`,
    '-r', String(VIDEO_FPS),
    '-i', 'pipe:0',
    '-c:v', 'libvpx', '-b:v', '1500k',
    '-deadline', 'realtime', '-cpu-used', '8',
    '-an', vOut,
  ], { stdio: ['pipe', 'inherit', 'inherit'] });

  vProc.on('exit', (code) => console.log(`[lk-rec] FFmpeg video seg${idx} exited (code=${code})`));

  const aProc = spawn('ffmpeg', [
    '-y',
    '-f', 's16le', '-ar', String(AUDIO_SAMPLE_RATE), '-ac', String(AUDIO_CHANNELS),
    '-i', 'pipe:0',
    '-c:a', 'libopus', '-b:a', '128k',
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
  const mergedOut = segmentPath(oldIdx, '.webm');
  mergeSegment(oldVOut, oldAOut, mergedOut, oldIdx);

  segmentRolling = false;
}

function mergeSegment(vOut, aOut, mergedOut, idx) {
  const vExists = vOut && fs.existsSync(vOut) && fs.statSync(vOut).size > 0;
  const aExists = aOut && fs.existsSync(aOut) && fs.statSync(aOut).size > 0;

  let args;
  if (vExists && aExists) {
    args = ['-y', '-i', vOut, '-i', aOut, '-c:v', 'copy', '-c:a', 'copy', mergedOut];
  } else if (vExists) {
    args = ['-y', '-i', vOut, '-c', 'copy', mergedOut];
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
            console.log(`[lk-rec] First video frame: ${actualWidth}x${actualHeight}`);
            // Start first FFmpeg segment now that we know the resolution
            const { vProc, aProc, vOut, aOut } = startFFmpegSegment(0, actualWidth, actualHeight);
            videoProc       = vProc;
            audioProc       = aProc;
            videoStdin      = vProc.stdin;
            audioStdin      = aProc.stdin;
            currentVideoOut = vOut;
            currentAudioOut = aOut;
            segmentStartMs  = Date.now();
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
              // Roll segment if time limit reached
              if (SEGMENT_MS > 0 && !segmentRolling && (Date.now() - segmentStartMs) >= SEGMENT_MS) {
                rollSegment();  // async, non-blocking
              }
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

  (async () => {
    try {
      while (!stopping) {
        const { done, value: frame } = await reader.read();
        if (done || stopping) break;
        try {
          if (!audioStdin || audioStdin.destroyed) continue;
          const buf = Buffer.from(frame.data.buffer, frame.data.byteOffset, frame.data.byteLength);
          audioStdin.write(buf);
          audioFrameCount++;
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
  const finalMerged = segmentPath(segmentIndex, '.webm');
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
    console.log(`[lk-rec] TrackSubscribed: kind=${track.kind} from ${participant.identity}`);
    if (track.kind === TrackKind.KIND_VIDEO && !videoStream) {
      startVideoCapture(track);
    } else if (track.kind === TrackKind.KIND_AUDIO && !audioStream) {
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
      console.log(`[lk-rec] Existing track: kind=${pub.kind} subscribed=${pub.subscribed} muted=${pub.muted}`);
      if (!pub.subscribed) {
        try { pub.setSubscribed(true); console.log(`[lk-rec] Called setSubscribed(true) on ${pub.sid}`); }
        catch (e) { console.warn('[lk-rec] setSubscribed error:', e.message); }
      }
    }
  }

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
