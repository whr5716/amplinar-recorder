#!/usr/bin/env node
/**
 * livekit_recorder.js
 * ===================
 * Connects to a LiveKit room as a silent participant, subscribes to the
 * avatar agent's video and audio tracks, and writes raw frames to temp files
 * which are then muxed by FFmpeg into a WebM file.
 *
 * Usage:
 *   node livekit_recorder.js <lk_url> <lk_token> <output_path>
 *
 * SIGTERM → gracefully stop capture and mux output.
 *
 * Key fix (v2): TrackSubscribed handler uses detached async IIFEs for the
 * frame loops so they don't block the event emitter. The original code used
 * `for await` directly inside the event handler which prevented the second
 * track's TrackSubscribed event from ever firing.
 */

'use strict';

const {
  Room,
  RoomEvent,
  TrackKind,
  VideoStream,
  AudioStream,
} = require('@livekit/rtc-node');

const { spawn, spawnSync } = require('child_process');
const fs = require('fs');

// ── Args ──────────────────────────────────────────────────────────────────────
const [,, LK_URL, LK_TOKEN, OUTPUT_PATH] = process.argv;
if (!LK_URL || !LK_TOKEN || !OUTPUT_PATH) {
  console.error('[lk-rec] Usage: node livekit_recorder.js <lk_url> <lk_token> <output_path>');
  process.exit(1);
}

// ── Video/audio config ────────────────────────────────────────────────────────
const VIDEO_FPS         = 30;
const AUDIO_SAMPLE_RATE = 48000;
const AUDIO_CHANNELS    = 1;

// ── State ─────────────────────────────────────────────────────────────────────
let stopping = false;
let videoStream = null;
let audioStream = null;
let room = null;

let videoFrameCount = 0;
let audioFrameCount = 0;
let actualWidth  = 1280;
let actualHeight = 720;

// Temp raw files
const videoTmp = OUTPUT_PATH + '.video.raw';
const audioTmp = OUTPUT_PATH + '.audio.raw';
let videoFd = null;
let audioFd = null;

// ── Open temp files ───────────────────────────────────────────────────────────
try {
  videoFd = fs.openSync(videoTmp, 'w');
  audioFd = fs.openSync(audioTmp, 'w');
} catch (e) {
  console.error('[lk-rec] Failed to open temp files:', e.message);
  process.exit(1);
}

// ── Track handlers (detached — do NOT await inside the event handler) ─────────
function startVideoCapture(track) {
  console.log('[lk-rec] Starting video capture');
  videoStream = new VideoStream(track);

  // Detached async loop — runs independently of the event emitter
  (async () => {
    try {
      for await (const frame of videoStream) {
        if (stopping) break;
        try {
          const i420 = frame.buffer.toI420();
          actualWidth  = i420.width;
          actualHeight = i420.height;
          fs.writeSync(videoFd, Buffer.from(i420.dataY));
          fs.writeSync(videoFd, Buffer.from(i420.dataU));
          fs.writeSync(videoFd, Buffer.from(i420.dataV));
          videoFrameCount++;
          if (videoFrameCount % 300 === 0) {
            console.log(`[lk-rec] Video frames: ${videoFrameCount} (${actualWidth}x${actualHeight})`);
          }
        } catch (e) {
          if (!stopping) console.error('[lk-rec] Video frame error:', e.message);
        }
      }
    } catch (e) {
      if (!stopping) console.error('[lk-rec] Video stream error:', e.message);
    }
    console.log(`[lk-rec] Video capture ended (${videoFrameCount} frames)`);
  })();
}

function startAudioCapture(track) {
  console.log('[lk-rec] Starting audio capture');
  audioStream = new AudioStream(track, AUDIO_SAMPLE_RATE, AUDIO_CHANNELS);

  // Detached async loop — runs independently of the event emitter
  (async () => {
    try {
      for await (const frame of audioStream) {
        if (stopping) break;
        try {
          const buf = Buffer.from(frame.data.buffer);
          fs.writeSync(audioFd, buf);
          audioFrameCount++;
        } catch (e) {
          if (!stopping) console.error('[lk-rec] Audio frame error:', e.message);
        }
      }
    } catch (e) {
      if (!stopping) console.error('[lk-rec] Audio stream error:', e.message);
    }
    console.log(`[lk-rec] Audio capture ended (${audioFrameCount} frames)`);
  })();
}

// ── Graceful stop ─────────────────────────────────────────────────────────────
async function gracefulStop() {
  if (stopping) return;
  stopping = true;

  console.log('[lk-rec] Stopping capture...');

  // Close streams (this ends the for-await loops)
  if (videoStream) { try { videoStream.close(); } catch (_) {} }
  if (audioStream) { try { audioStream.close(); } catch (_) {} }

  // Small delay to let the loops drain their last frames
  await new Promise(r => setTimeout(r, 500));

  // Disconnect from room
  if (room) { try { await room.disconnect(); } catch (_) {} }

  // Close file descriptors
  if (videoFd !== null) { try { fs.closeSync(videoFd); } catch (_) {} videoFd = null; }
  if (audioFd !== null) { try { fs.closeSync(audioFd); } catch (_) {} audioFd = null; }

  console.log(`[lk-rec] Raw data: ${videoFrameCount} video frames, ${audioFrameCount} audio frames`);

  if (videoFrameCount === 0 && audioFrameCount === 0) {
    console.error('[lk-rec] No frames captured — output will be empty');
    // Clean up
    try { fs.unlinkSync(videoTmp); } catch (_) {}
    try { fs.unlinkSync(audioTmp); } catch (_) {}
    process.exit(1);
  }

  // Mux with FFmpeg
  console.log('[lk-rec] Muxing with FFmpeg...');

  let ffArgs;
  if (videoFrameCount > 0 && audioFrameCount > 0) {
    ffArgs = [
      '-y',
      '-f', 'rawvideo',
      '-pix_fmt', 'yuv420p',
      '-s', `${actualWidth}x${actualHeight}`,
      '-r', String(VIDEO_FPS),
      '-i', videoTmp,
      '-f', 's16le',
      '-ar', String(AUDIO_SAMPLE_RATE),
      '-ac', String(AUDIO_CHANNELS),
      '-i', audioTmp,
      '-c:v', 'libvpx',
      '-b:v', '1500k',
      '-deadline', 'realtime',
      '-cpu-used', '8',
      '-c:a', 'libopus',
      '-b:a', '128k',
      OUTPUT_PATH,
    ];
  } else if (videoFrameCount > 0) {
    // Video only
    ffArgs = [
      '-y',
      '-f', 'rawvideo',
      '-pix_fmt', 'yuv420p',
      '-s', `${actualWidth}x${actualHeight}`,
      '-r', String(VIDEO_FPS),
      '-i', videoTmp,
      '-c:v', 'libvpx',
      '-b:v', '1500k',
      '-deadline', 'realtime',
      '-cpu-used', '8',
      '-an',
      OUTPUT_PATH,
    ];
  } else {
    // Audio only
    ffArgs = [
      '-y',
      '-f', 's16le',
      '-ar', String(AUDIO_SAMPLE_RATE),
      '-ac', String(AUDIO_CHANNELS),
      '-i', audioTmp,
      '-c:a', 'libopus',
      '-b:a', '128k',
      OUTPUT_PATH,
    ];
  }

  const result = spawnSync('ffmpeg', ffArgs, { stdio: 'inherit', timeout: 300000 });
  if (result.status !== 0) {
    console.error(`[lk-rec] FFmpeg mux failed with code ${result.status}`);
  } else {
    try {
      const stat = fs.statSync(OUTPUT_PATH);
      console.log(`[lk-rec] Output: ${OUTPUT_PATH} (${stat.size} bytes)`);
    } catch (_) {}
  }

  // Clean up temp files
  try { fs.unlinkSync(videoTmp); } catch (_) {}
  try { fs.unlinkSync(audioTmp); } catch (_) {}

  process.exit(result.status || 0);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[lk-rec] Connecting to ${LK_URL}`);
  console.log(`[lk-rec] Output: ${OUTPUT_PATH}`);

  room = new Room();

  // Register TrackSubscribed BEFORE connecting so we catch events that fire
  // immediately after connect for already-published tracks.
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

  // Connect with autoSubscribe:true so the server auto-subscribes us to all tracks
  await room.connect(LK_URL, LK_TOKEN, { autoSubscribe: true });
  console.log(`[lk-rec] Connected to room: ${room.name}, participants: ${room.remoteParticipants.size}`);

  // Scan existing participants — with autoSubscribe:true the server should send
  // TrackSubscribed events shortly after connect, but we also call setSubscribed(true)
  // on any unsubscribed publications as a belt-and-suspenders measure.
  for (const [, participant] of room.remoteParticipants) {
    console.log(`[lk-rec] Existing participant: ${participant.identity} (${participant.trackPublications.size} tracks)`);
    for (const [, pub] of participant.trackPublications) {
      // Note: the property is `subscribed` (not `isSubscribed`) in @livekit/rtc-node
      console.log(`[lk-rec] Existing track: kind=${pub.kind} subscribed=${pub.subscribed} muted=${pub.muted}`);
      if (!pub.subscribed) {
        try {
          pub.setSubscribed(true);
          console.log(`[lk-rec] Called setSubscribed(true) on ${pub.sid}`);
        } catch (e) {
          console.warn('[lk-rec] setSubscribed error:', e.message);
        }
      }
    }
  }

  // Wait for stop signal
  await new Promise(resolve => {
    process.once('SIGTERM', () => {
      console.log('[lk-rec] SIGTERM received — stopping');
      resolve();
    });
    process.once('SIGINT', () => {
      console.log('[lk-rec] SIGINT received — stopping');
      resolve();
    });
  });

  await gracefulStop();
}

main().catch(e => {
  console.error('[lk-rec] Fatal:', e);
  process.exit(1);
});
