'use strict';

/**
 * livekit_recorder.js  (v4 — streaming FFmpeg pipeline)
 * ======================================================
 * Connects to a LiveKit room as a silent subscriber, receives the avatar's
 * video and audio tracks, and pipes raw frames **directly into FFmpeg stdin
 * in real time**.  FFmpeg writes the output WebM file incrementally, so:
 *
 *   • A partial recording is always on disk — even if the process crashes
 *     mid-session, the file is a valid (truncated) WebM.
 *   • No large temp buffers in /tmp — frames are never accumulated.
 *   • Graceful stop just closes the FFmpeg stdin pipes; FFmpeg finalises
 *     the container and exits cleanly.
 *
 * Usage:
 *   node livekit_recorder.js <lk_url> <lk_room> <output_path>
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
const { spawn }       = require('child_process');
const fs              = require('fs');

// ── Args ──────────────────────────────────────────────────────────────────────
const [,, LK_URL, LK_ROOM, OUTPUT_PATH] = process.argv;
if (!LK_URL || !LK_ROOM || !OUTPUT_PATH) {
  console.error('[lk-rec] Usage: node livekit_recorder.js <lk_url> <lk_room> <output_path>');
  process.exit(1);
}

const LK_API_KEY    = process.env.LIVEKIT_API_KEY;
const LK_API_SECRET = process.env.LIVEKIT_API_SECRET;
if (!LK_API_KEY || !LK_API_SECRET) {
  console.error('[lk-rec] LIVEKIT_API_KEY and LIVEKIT_API_SECRET env vars are required');
  process.exit(1);
}

// ── Token ─────────────────────────────────────────────────────────────────────
async function makeToken(room) {
  const identity = `amplinar-recorder-${Date.now()}`;
  const at = new AccessToken(LK_API_KEY, LK_API_SECRET, { identity, ttl: '4h' });
  at.addGrant({ roomJoin: true, room, canPublish: false, canSubscribe: true, canPublishData: false });
  return at.toJwt();
}

// ── Config ────────────────────────────────────────────────────────────────────
const VIDEO_FPS         = 30;
const AUDIO_SAMPLE_RATE = 48000;
const AUDIO_CHANNELS    = 1;

// ── State ─────────────────────────────────────────────────────────────────────
let stopping        = false;
let room            = null;
let videoStream     = null;
let audioStream     = null;
let ffmpegProc      = null;   // single FFmpeg process (video+audio)
let videoStdin      = null;   // writable pipe for raw YUV frames
let audioStdin      = null;   // writable pipe for raw PCM frames (separate process)
let videoFrameCount = 0;
let audioFrameCount = 0;
let actualWidth     = 1280;
let actualHeight    = 720;
let ffmpegStarted   = false;

// We use two separate FFmpeg processes piped together via named pipes, OR
// we use a single FFmpeg with two stdin inputs via /dev/fd/N.  The simplest
// reliable approach on Linux is two separate FFmpeg processes:
//   1. video-only FFmpeg  → raw YUV → libvpx → video.webm
//   2. audio-only FFmpeg  → raw PCM → libopus → audio.webm
// Then a final FFmpeg merge step at the end.
//
// Even simpler: use a single FFmpeg with -f rawvideo piped to fd 0 for video
// and a named pipe (FIFO) for audio.  But the most robust approach for
// crash-safety is to write video and audio to separate WebM files and merge
// at the end — if the process crashes, both partial files are valid WebMs.

const videoOut = OUTPUT_PATH + '.video.webm';
const audioOut = OUTPUT_PATH + '.audio.webm';

// ── Start FFmpeg processes ────────────────────────────────────────────────────
function startFFmpeg() {
  if (ffmpegStarted) return;
  ffmpegStarted = true;

  console.log('[lk-rec] Starting FFmpeg video process');
  const videoProc = spawn('ffmpeg', [
    '-y',
    '-f', 'rawvideo',
    '-pix_fmt', 'yuv420p',
    '-s', `${actualWidth}x${actualHeight}`,
    '-r', String(VIDEO_FPS),
    '-i', 'pipe:0',          // read raw YUV from stdin
    '-c:v', 'libvpx',
    '-b:v', '1500k',
    '-deadline', 'realtime',
    '-cpu-used', '8',
    '-an',
    videoOut,
  ], { stdio: ['pipe', 'inherit', 'inherit'] });

  videoProc.on('exit', (code) => {
    console.log(`[lk-rec] FFmpeg video exited (code=${code})`);
  });
  videoStdin = videoProc.stdin;

  console.log('[lk-rec] Starting FFmpeg audio process');
  const audioProc = spawn('ffmpeg', [
    '-y',
    '-f', 's16le',
    '-ar', String(AUDIO_SAMPLE_RATE),
    '-ac', String(AUDIO_CHANNELS),
    '-i', 'pipe:0',          // read raw PCM from stdin
    '-c:a', 'libopus',
    '-b:a', '128k',
    audioOut,
  ], { stdio: ['pipe', 'inherit', 'inherit'] });

  audioProc.on('exit', (code) => {
    console.log(`[lk-rec] FFmpeg audio exited (code=${code})`);
  });
  audioStdin = audioProc.stdin;
}

// ── Track capture loops ───────────────────────────────────────────────────────
function startVideoCapture(track) {
  if (videoStream) return;
  console.log('[lk-rec] Starting video capture');

  // We need the first frame to know the resolution before starting FFmpeg.
  // Capture one frame, set dimensions, start FFmpeg, then continue.
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
            startFFmpeg();
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
    // Close video stdin so FFmpeg knows the stream is done
    if (videoStdin && !videoStdin.destroyed) {
      videoStdin.end();
    }
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
          // AudioStream enqueues AudioFrame directly (not a {frame} wrapper)
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
    // Close audio stdin so FFmpeg knows the stream is done
    if (audioStdin && !audioStdin.destroyed) {
      audioStdin.end();
    }
  })();
}

// ── Graceful stop ─────────────────────────────────────────────────────────────
async function gracefulStop() {
  if (stopping) return;
  stopping = true;

  console.log('[lk-rec] Stopping capture...');

  // Signal reader loops to exit by setting stopping=true (already done above).
  // Wait for them to drain — they check stopping at the top of each iteration.
  // The loops will call videoStdin.end() / audioStdin.end() when they exit,
  // which tells FFmpeg to finalise the output files.
  await new Promise(r => setTimeout(r, 3000));

  // Force-close stdin pipes if the loops didn't (belt-and-suspenders)
  if (videoStdin && !videoStdin.destroyed) { try { videoStdin.end(); } catch (_) {} }
  if (audioStdin && !audioStdin.destroyed) { try { audioStdin.end(); } catch (_) {} }

  // Give FFmpeg time to finalise the output files
  await new Promise(r => setTimeout(r, 5000));

  // Disconnect from room
  if (room) { try { await room.disconnect(); } catch (_) {} }

  console.log(`[lk-rec] Raw data: ${videoFrameCount} video frames, ${audioFrameCount} audio frames`);

  if (videoFrameCount === 0 && audioFrameCount === 0) {
    console.error('[lk-rec] No frames captured — output will be empty');
    process.exit(1);
  }

  // Merge video and audio WebM files into the final output
  console.log('[lk-rec] Merging video and audio...');

  const videoExists = fs.existsSync(videoOut) && fs.statSync(videoOut).size > 0;
  const audioExists = fs.existsSync(audioOut) && fs.statSync(audioOut).size > 0;

  let mergeArgs;
  if (videoExists && audioExists) {
    mergeArgs = [
      '-y',
      '-i', videoOut,
      '-i', audioOut,
      '-c:v', 'copy',
      '-c:a', 'copy',
      OUTPUT_PATH,
    ];
  } else if (videoExists) {
    mergeArgs = ['-y', '-i', videoOut, '-c', 'copy', OUTPUT_PATH];
  } else if (audioExists) {
    mergeArgs = ['-y', '-i', audioOut, '-c', 'copy', OUTPUT_PATH];
  } else {
    console.error('[lk-rec] No output files produced by FFmpeg');
    process.exit(1);
  }

  const { spawnSync } = require('child_process');
  const result = spawnSync('ffmpeg', mergeArgs, { stdio: 'inherit', timeout: 120000 });

  if (result.status !== 0) {
    console.error(`[lk-rec] FFmpeg merge failed (code=${result.status})`);
    // Still try to use whichever partial file exists
    if (videoExists) {
      fs.copyFileSync(videoOut, OUTPUT_PATH);
      console.log('[lk-rec] Falling back to video-only output');
    }
  }

  // Clean up intermediate files
  try { fs.unlinkSync(videoOut); } catch (_) {}
  try { fs.unlinkSync(audioOut); } catch (_) {}

  try {
    const stat = fs.statSync(OUTPUT_PATH);
    console.log(`[lk-rec] Output: ${OUTPUT_PATH} (${stat.size} bytes)`);
  } catch (_) {
    console.error('[lk-rec] Output file not found after merge');
    process.exit(1);
  }

  process.exit(0);
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log(`[lk-rec] Connecting to ${LK_URL} room=${LK_ROOM}`);
  console.log(`[lk-rec] Output: ${OUTPUT_PATH}`);
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

  // Scan existing participants and subscribe to any unsubscribed tracks
  for (const [, participant] of room.remoteParticipants) {
    console.log(`[lk-rec] Existing participant: ${participant.identity} (${participant.trackPublications.size} tracks)`);
    for (const [, pub] of participant.trackPublications) {
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
  // Even on fatal error, try to close FFmpeg stdin so partial output is saved
  if (videoStdin && !videoStdin.destroyed) { try { videoStdin.end(); } catch (_) {} }
  if (audioStdin && !audioStdin.destroyed) { try { audioStdin.end(); } catch (_) {} }
  process.exit(1);
});
