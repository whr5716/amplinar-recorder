#!/usr/bin/env node
/**
 * livekit_recorder.js
 * ===================
 * Connects to a LiveKit room as a silent participant, subscribes to the
 * avatar agent's video and audio tracks, and pipes raw frames into FFmpeg
 * to produce a WebM file.
 *
 * Usage:
 *   node livekit_recorder.js <lk_url> <lk_token> <output_path>
 *
 * Env:
 *   LIVEKIT_URL        wss://amplinar-jasxfeql.livekit.cloud
 *   LIVEKIT_API_KEY    APIbi8w4saPGNm4
 *   LIVEKIT_API_SECRET NVFZe6S6BHvPtiZL1XFwfXOE5C2BpqNZQrfakE0nlzLB
 *
 * SIGTERM → gracefully stop FFmpeg and exit.
 */

'use strict';

const {
  Room,
  RoomEvent,
  RemoteTrack,
  TrackKind,
  VideoStream,
  AudioStream,
  VideoBufferType,
} = require('@livekit/rtc-node');

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

// ── Args ──────────────────────────────────────────────────────────────────────
const [,, LK_URL, LK_TOKEN, OUTPUT_PATH] = process.argv;
if (!LK_URL || !LK_TOKEN || !OUTPUT_PATH) {
  console.error('[lk-rec] Usage: node livekit_recorder.js <lk_url> <lk_token> <output_path>');
  process.exit(1);
}

// ── Video config ──────────────────────────────────────────────────────────────
// LiveKit avatar is typically 720p30 or 1080p30; we request I420 raw frames.
const VIDEO_WIDTH  = 1280;
const VIDEO_HEIGHT = 720;
const VIDEO_FPS    = 30;
const AUDIO_SAMPLE_RATE = 48000;
const AUDIO_CHANNELS    = 1;

// ── State ─────────────────────────────────────────────────────────────────────
let ffmpeg = null;
let stopping = false;
let videoStream = null;
let audioStream = null;
let room = null;

// ── FFmpeg process ─────────────────────────────────────────────────────────────
function startFFmpeg(outputPath) {
  // Input: raw video (I420) on fd 3, raw audio (s16le) on fd 4
  // We use named pipes (fifos) for video and audio since FFmpeg can't read
  // two streams from a single stdin. Instead we use two child_process pipes
  // via FFmpeg's pipe: protocol.
  //
  // Simpler approach: write video frames to a temp rawvideo file and audio
  // frames to a temp pcm file, then mux at the end. But that uses lots of
  // disk. Instead we use FFmpeg's concat protocol with two fifos.
  //
  // Simplest reliable approach for Node.js → FFmpeg: use two separate
  // FFmpeg processes (one for video, one for audio) and mux at the end.
  // But that's complex too.
  //
  // Best approach for streaming: use FFmpeg with -f rawvideo piped on stdin
  // for video only, and capture audio separately as PCM, then mux.
  // We'll write audio to a temp PCM file and video via stdin.
  //
  // Actually the cleanest: write both to temp files and mux at end.
  // For a session that's 30-60 min this is ~2-4 GB raw — too much.
  //
  // Use FFmpeg's lavfi concat or pipe approach:
  // We'll use a single FFmpeg process with two named pipes.

  const videoFifo = outputPath + '.video.fifo';
  const audioFifo = outputPath + '.audio.fifo';

  // Create fifos
  try { fs.unlinkSync(videoFifo); } catch (_) {}
  try { fs.unlinkSync(audioFifo); } catch (_) {}

  const { execSync } = require('child_process');
  execSync(`mkfifo "${videoFifo}"`);
  execSync(`mkfifo "${audioFifo}"`);

  const ff = spawn('ffmpeg', [
    '-y',
    // Video input: raw I420 frames from fifo
    '-f', 'rawvideo',
    '-pix_fmt', 'yuv420p',
    '-s', `${VIDEO_WIDTH}x${VIDEO_HEIGHT}`,
    '-r', String(VIDEO_FPS),
    '-i', videoFifo,
    // Audio input: raw s16le PCM from fifo
    '-f', 's16le',
    '-ar', String(AUDIO_SAMPLE_RATE),
    '-ac', String(AUDIO_CHANNELS),
    '-i', audioFifo,
    // Output: WebM with VP8 + Opus
    '-c:v', 'libvpx',
    '-b:v', '1500k',
    '-crf', '10',
    '-deadline', 'realtime',
    '-cpu-used', '8',
    '-c:a', 'libopus',
    '-b:a', '128k',
    '-ar', '48000',
    outputPath,
  ], { stdio: ['ignore', 'pipe', 'pipe'] });

  ff.stdout.on('data', d => process.stdout.write(d));
  ff.stderr.on('data', d => process.stderr.write(d));
  ff.on('exit', (code) => {
    console.log(`[lk-rec] FFmpeg exited with code ${code}`);
    // Clean up fifos
    try { fs.unlinkSync(videoFifo); } catch (_) {}
    try { fs.unlinkSync(audioFifo); } catch (_) {}
  });

  return { ff, videoFifo, audioFifo };
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[lk-rec] Connecting to ${LK_URL}`);
  console.log(`[lk-rec] Output: ${OUTPUT_PATH}`);

  // Use temp files for video and audio, then mux at the end.
  // This avoids the fifo complexity and is more reliable.
  const videoTmp = OUTPUT_PATH + '.video.raw';
  const audioTmp = OUTPUT_PATH + '.audio.raw';

  const videoFd = fs.openSync(videoTmp, 'w');
  const audioFd = fs.openSync(audioTmp, 'w');

  let videoTrackFound = false;
  let audioTrackFound = false;
  let videoFrameCount = 0;
  let audioFrameCount = 0;
  let actualWidth = VIDEO_WIDTH;
  let actualHeight = VIDEO_HEIGHT;

  room = new Room();

  room.on(RoomEvent.TrackSubscribed, async (track, publication, participant) => {
    console.log(`[lk-rec] Track subscribed: ${track.kind} from ${participant.identity}`);

    if (track.kind === TrackKind.KIND_VIDEO && !videoTrackFound) {
      videoTrackFound = true;
      console.log('[lk-rec] Starting video capture');
      videoStream = new VideoStream(track);
      for await (const frame of videoStream) {
        if (stopping) break;
        try {
          // frame.buffer is a VideoFrame — get I420 data
          const i420 = frame.buffer.toI420();
          actualWidth  = i420.width;
          actualHeight = i420.height;
          // Write Y, U, V planes
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
      console.log(`[lk-rec] Video capture ended (${videoFrameCount} frames)`);
    }

    if (track.kind === TrackKind.KIND_AUDIO && !audioTrackFound) {
      audioTrackFound = true;
      console.log('[lk-rec] Starting audio capture');
      audioStream = new AudioStream(track, AUDIO_SAMPLE_RATE, AUDIO_CHANNELS);
      for await (const frame of audioStream) {
        if (stopping) break;
        try {
          // frame is an AudioFrame — data is Int16Array
          const buf = Buffer.from(frame.data.buffer);
          fs.writeSync(audioFd, buf);
          audioFrameCount++;
        } catch (e) {
          if (!stopping) console.error('[lk-rec] Audio frame error:', e.message);
        }
      }
      console.log(`[lk-rec] Audio capture ended (${audioFrameCount} frames)`);
    }
  });

  room.on(RoomEvent.TrackUnsubscribed, (track, publication, participant) => {
    console.log(`[lk-rec] Track unsubscribed: ${track.kind} from ${participant.identity}`);
  });

  room.on(RoomEvent.Disconnected, (reason) => {
    console.log(`[lk-rec] Disconnected: ${reason}`);
    if (!stopping) {
      console.log('[lk-rec] Unexpected disconnect — stopping');
      gracefulStop();
    }
  });

  await room.connect(LK_URL, LK_TOKEN, { autoSubscribe: true });
  console.log(`[lk-rec] Connected to room: ${room.name}, participants: ${room.remoteParticipants.size}`);

  // Scan existing participants — if the avatar agent is already publishing,
  // TrackSubscribed may not fire retroactively. Manually subscribe to any
  // already-published tracks.
  for (const [, participant] of room.remoteParticipants) {
    console.log(`[lk-rec] Existing participant: ${participant.identity} (${participant.trackPublications.size} tracks)`);
    for (const [, pub] of participant.trackPublications) {
      console.log(`[lk-rec] Existing track: kind=${pub.kind} subscribed=${pub.isSubscribed} muted=${pub.isMuted}`);
      if (!pub.isSubscribed) {
        try { pub.setSubscribed(true); } catch (e) { console.warn('[lk-rec] setSubscribed error:', e.message); }
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

  async function gracefulStop() {
    if (stopping) return;
    stopping = true;

    console.log('[lk-rec] Stopping capture...');

    // Close streams
    if (videoStream) { try { videoStream.close(); } catch (_) {} }
    if (audioStream) { try { audioStream.close(); } catch (_) {} }

    // Disconnect from room
    if (room) { try { await room.disconnect(); } catch (_) {} }

    // Close file descriptors
    try { fs.closeSync(videoFd); } catch (_) {}
    try { fs.closeSync(audioFd); } catch (_) {}

    console.log(`[lk-rec] Raw data: ${videoFrameCount} video frames, ${audioFrameCount} audio frames`);

    if (videoFrameCount === 0 && audioFrameCount === 0) {
      console.error('[lk-rec] No frames captured — output will be empty');
      process.exit(1);
    }

    // Mux with FFmpeg
    console.log('[lk-rec] Muxing with FFmpeg...');
    const { spawnSync } = require('child_process');

    const ffArgs = [
      '-y',
      // Video
      '-f', 'rawvideo',
      '-pix_fmt', 'yuv420p',
      '-s', `${actualWidth}x${actualHeight}`,
      '-r', String(VIDEO_FPS),
      '-i', videoTmp,
      // Audio
      '-f', 's16le',
      '-ar', String(AUDIO_SAMPLE_RATE),
      '-ac', String(AUDIO_CHANNELS),
      '-i', audioTmp,
      // Output
      '-c:v', 'libvpx',
      '-b:v', '1500k',
      '-deadline', 'realtime',
      '-cpu-used', '8',
      '-c:a', 'libopus',
      '-b:a', '128k',
      OUTPUT_PATH,
    ];

    // If no video, output audio only
    if (videoFrameCount === 0) {
      ffArgs.splice(0, ffArgs.length,
        '-y',
        '-f', 's16le', '-ar', String(AUDIO_SAMPLE_RATE), '-ac', String(AUDIO_CHANNELS),
        '-i', audioTmp,
        '-c:a', 'libopus', '-b:a', '128k',
        OUTPUT_PATH
      );
    }

    const result = spawnSync('ffmpeg', ffArgs, { stdio: 'inherit', timeout: 120000 });
    if (result.status !== 0) {
      console.error(`[lk-rec] FFmpeg mux failed with code ${result.status}`);
    } else {
      const stat = fs.statSync(OUTPUT_PATH);
      console.log(`[lk-rec] Output: ${OUTPUT_PATH} (${stat.size} bytes)`);
    }

    // Clean up temp files
    try { fs.unlinkSync(videoTmp); } catch (_) {}
    try { fs.unlinkSync(audioTmp); } catch (_) {}

    process.exit(result.status || 0);
  }
}

main().catch(e => {
  console.error('[lk-rec] Fatal:', e);
  process.exit(1);
});
