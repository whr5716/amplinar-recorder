"""
Amplinar Recorder Service — LiveKit Direct Capture
===================================================
Records a live Amplinar session by connecting directly to the LiveKit room
and capturing avatar video + audio tracks. Video segments (played locally
in viewer browsers) are downloaded by URL and spliced in at the correct
position. All segments are stitched with FFmpeg into one final MP4 file
and uploaded to S3.

Architecture
------------
- On /start: generate a LiveKit access token for the recorder participant,
  spawn livekit_recorder.js to capture the room's audio+video tracks
- Listen to the relay WebSocket for segment events:
    video_segment      → pause LiveKit capture, download the video file
    video_segment_end  → resume LiveKit capture
- On /stop: SIGTERM the Node.js worker, stitch all segments, upload to S3

Hardening (v2)
--------------
- Every segment is uploaded to S3 immediately after capture — raw pieces
  are always safe even if the final stitch fails
- Final MP4 is streamed to disk (not held in memory) — no OOM on large files
- Segment files are NEVER deleted on failure — only on successful completion
- /retry endpoint re-stitches from S3 segment URLs without redeploying
- Fixed NameError in segment-complete callback (rec referenced before assign)
- Fixed TypeError in duration calculation (ISO string vs datetime)

API
---
  POST /start   { "session_id": "...", "amplinar_id": "...", "room_name": "..." }
  POST /stop    { "session_id": "..." }
  POST /retry   { "session_id": "...", "amplinar_id": "...", "segment_urls": [...] }
  GET  /status
  GET  /health
"""
from __future__ import annotations

import glob as _glob
import json
import logging
import os
import signal
import subprocess
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
import websocket  # websocket-client
from flask import Flask, jsonify, request

# LiveKit token generation
import jwt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("amplinar-recorder")

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
LIVEKIT_URL          = os.environ.get("LIVEKIT_URL", "")
LIVEKIT_API_KEY      = os.environ.get("LIVEKIT_API_KEY", "")
LIVEKIT_API_SECRET   = os.environ.get("LIVEKIT_API_SECRET", "")
RECORDER_API_KEY     = os.environ.get("RECORDER_API_KEY", "")
S3_ACCESS_KEY_ID     = os.environ.get("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME       = os.environ.get("S3_BUCKET_NAME", "wholesalehotelrates-images")
S3_REGION            = os.environ.get("S3_REGION", "us-east-1")
RELAY_URL            = os.environ.get("RELAY_URL", "")
RELAY_API_KEY        = os.environ.get("RECORDER_API_KEY", "")

LK_RECORDER_JS = os.path.join(os.path.dirname(__file__), "livekit_recorder.js")

# ── State ─────────────────────────────────────────────────────────────────────
_recording: Optional[dict] = None
_recording_lock = threading.Lock()


# ── Auth ──────────────────────────────────────────────────────────────────────
def check_auth() -> bool:
    if not RECORDER_API_KEY:
        return True
    return request.headers.get("X-Recorder-Key") == RECORDER_API_KEY


# ── LiveKit token ─────────────────────────────────────────────────────────────
def _make_lk_token(room_name: str, identity: str = "amplinar-recorder") -> str:
    now = int(time.time())
    payload = {
        "iss": LIVEKIT_API_KEY,
        "sub": identity,
        "iat": now,
        "exp": now + 7200,
        "video": {
            "room": room_name,
            "roomJoin": True,
            "canPublish": False,
            "canSubscribe": True,
            "canPublishData": False,
        },
    }
    return jwt.encode(payload, LIVEKIT_API_SECRET, algorithm="HS256")


# ── S3 helpers ────────────────────────────────────────────────────────────────
def _s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
    )


def upload_file_to_s3(local_path: str, s3_key: str) -> str:
    """Upload a local file to S3 using multipart streaming — no OOM risk."""
    s3 = _s3_client()
    size = os.path.getsize(local_path)
    logger.info(f"[S3] Uploading {size:,} bytes from {local_path} → {s3_key}")
    s3.upload_file(
        local_path, S3_BUCKET_NAME, s3_key,
        ExtraArgs={"ContentType": "video/mp4"},
    )
    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
    logger.info(f"[S3] Done: {url}")
    return url


def upload_bytes_to_s3(data: bytes, s3_key: str) -> str:
    """Upload bytes to S3 (kept for small payloads only)."""
    s3 = _s3_client()
    logger.info(f"[S3] Uploading {len(data):,} bytes → {s3_key}")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=data, ContentType="video/mp4")
    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
    logger.info(f"[S3] Done: {url}")
    return url


# ── Relay notification ────────────────────────────────────────────────────────
def _notify_relay(session_id: str, amplinar_id: str, recording_url: str,
                  title: str = "", duration_seconds: int = 0,
                  recording_type: str = None, scheduled_at: str = None) -> None:
    if not RELAY_URL:
        return
    try:
        payload = {
            "session_id":       session_id,
            "amplinar_id":      amplinar_id,
            "title":            title,
            "s3_url":           recording_url,
            "duration_seconds": duration_seconds,
            "recorded_at":      datetime.now(timezone.utc).isoformat(),
        }
        if recording_type:
            payload["recording_type"] = recording_type
        if scheduled_at:
            payload["scheduled_at"] = scheduled_at
        resp = requests.post(
            f"{RELAY_URL}/api/session/recording-complete",
            json=payload,
            headers={"x-api-key": RELAY_API_KEY},
            timeout=10,
        )
        logger.info(f"[Recorder] Relay notified: {resp.status_code}")
    except Exception as e:
        logger.error(f"[Recorder] Relay notification failed: {e}")


# ── FFmpeg concat ─────────────────────────────────────────────────────────────
def _concat_segments_to_file(segment_paths: list, out_path: str) -> None:
    """Concatenate segment files into out_path using FFmpeg.

    Strategy:
      Always re-encode to H.264/AAC to ensure audio is preserved across all
      segments regardless of source codec (LiveKit captures use Opus/H.264,
      downloaded videos may use AAC/H.264 — stream-copy silently drops audio
      when codec parameters differ between segments).

    Writes directly to out_path — no large in-memory buffers.
    Raises RuntimeError on failure.
    """
    if len(segment_paths) == 1:
        import shutil
        shutil.copy2(segment_paths[0], out_path)
        return

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as listf:
        for p in segment_paths:
            listf.write(f"file '{p}'\n")
        list_path = listf.name

    try:
        # Re-encode to H.264/AAC — handles any mix of input codecs and ensures
        # audio is present in every segment of the output.
        result = subprocess.run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_path,
             "-c:v", "libx264", "-preset", "fast", "-crf", "23",
             "-c:a", "aac", "-b:a", "128k",
             "-movflags", "+faststart",
             out_path],
            capture_output=True,
        )
        if result.returncode != 0:
            logger.error(f"[FFmpeg] concat failed: {result.stderr.decode()[-500:]}")
            raise RuntimeError(f"FFmpeg concat failed: {result.stderr.decode()[-300:]}")
        logger.info("[FFmpeg] concat completed (re-encode H.264/AAC)")
    finally:
        try:
            os.unlink(list_path)
        except Exception:
            pass


# ── Incremental S3 upload for a captured segment ─────────────────────────────
def _upload_segment_to_s3(local_path: str, session_id: str, amplinar_id: str,
                           seg_index: int, seg_type: str = "lk") -> Optional[str]:
    """Upload one segment file to S3 immediately after capture.

    Returns the S3 URL on success, None on failure.
    NEVER deletes the local file — caller is responsible for cleanup.
    """
    try:
        now = datetime.now(timezone.utc)
        s3_key = (f"amplinar-recordings/{amplinar_id}/segments/"
                  f"{session_id}_{seg_type}_{seg_index:03d}_{now.strftime('%H%M%S')}.mp4")
        url = upload_file_to_s3(local_path, s3_key)
        logger.info(f"[Recorder] Segment {seg_index} ({seg_type}) uploaded: {url}")
        return url
    except Exception as e:
        logger.error(f"[Recorder] Segment {seg_index} upload failed (non-fatal): {e}")
        return None


# ── LiveKit recorder subprocess ───────────────────────────────────────────────
def _start_lk_subprocess(lk_url: str, room_name: str, output_path: str,
                         session_id: str = "", amplinar_id: str = "") -> subprocess.Popen:
    port = int(os.environ.get("PORT", 8080))
    callback_url = f"http://localhost:{port}/segment-complete"
    env = os.environ.copy()
    proc = subprocess.Popen(
        [
            "node", LK_RECORDER_JS,
            lk_url, room_name, output_path,
            "--segment-minutes=0",
            f"--callback-url={callback_url}",
            f"--callback-key={RECORDER_API_KEY}",
            f"--session-id={session_id}",
            f"--amplinar-id={amplinar_id}",
        ],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    logger.info(f"[Recorder] livekit_recorder.js started (pid={proc.pid})")

    def _live_stream():
        try:
            for line in proc.stdout:
                logger.info(f"[lk-rec] {line.rstrip()}")
        except Exception:
            pass

    t = threading.Thread(target=_live_stream, daemon=True)
    t.start()
    proc._stdout_thread = t
    return proc


def _stop_lk_subprocess(proc: subprocess.Popen, timeout: int = 120) -> bool:
    if proc.poll() is not None:
        return True
    try:
        proc.send_signal(signal.SIGTERM)
        logger.info(f"[Recorder] SIGTERM sent to pid={proc.pid}")
    except Exception as e:
        logger.warning(f"[Recorder] SIGTERM failed: {e}")
    try:
        proc.wait(timeout=timeout)
        t = getattr(proc, '_stdout_thread', None)
        if t:
            t.join(timeout=10)
        return proc.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error(f"[Recorder] Subprocess did not exit in {timeout}s — killing")
        proc.kill()
        return False


def _send_lk_command(rec: dict, cmd: str) -> None:
    proc = rec.get("lk_proc")
    if proc and proc.poll() is None:
        try:
            proc.stdin.write(cmd + "\n")
            proc.stdin.flush()
            logger.info(f"[Recorder] Sent command to lk-rec: {cmd}")
        except Exception as e:
            logger.warning(f"[Recorder] Failed to send command '{cmd}': {e}")


# ── Relay WebSocket listener ──────────────────────────────────────────────────
def _relay_ws_listener(rec: dict) -> None:
    relay_ws_url = RELAY_URL.replace("https://", "wss://").replace("http://", "ws://")
    relay_ws_url = f"{relay_ws_url}/ws?role=recorder&session_id={rec['session_id']}"
    logger.info(f"[WS] Connecting to relay: {relay_ws_url}")

    def on_message(ws_app, message):
        try:
            msg = json.loads(message)
        except Exception:
            return
        msg_type = msg.get("type", "")
        if msg_type == "video_segment":
            url   = msg.get("url", "")
            title = msg.get("title", "")
            logger.info(f"[WS] video_segment: {title} — {url}")
            if url:
                rec["pending_video_segment"] = {"url": url, "title": title}
                _send_lk_command(rec, "ROLL_SEGMENT")
                rec["in_video_segment"].set()
        elif msg_type == "segment_change":
            # Track the current segment title so LK segments are labeled correctly
            seg_data = msg.get("segment") or {}
            seg_title = seg_data.get("title") or msg.get("segmentTitle") or ""
            if seg_title:
                rec["current_segment_title"] = seg_title
                logger.info(f"[WS] segment_change: title={seg_title!r}")
        elif msg_type == "video_segment_end":
            logger.info("[WS] video_segment_end")
            rec["in_video_segment"].clear()
            rec["video_segment_ended"].set()
        elif msg_type == "session_stopped":
            logger.info("[WS] session_stopped received")
            rec["stop_event"].set()

    def on_error(ws_app, error):
        logger.warning(f"[WS] Error: {error}")

    def on_close(ws_app, code, msg):
        logger.info(f"[WS] Closed: {code} {msg}")

    def on_open(ws_app):
        logger.info("[WS] Connected to relay")

    ws_app = websocket.WebSocketApp(
        relay_ws_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    rec["_ws"] = ws_app

    def _run():
        while not rec["stop_event"].is_set():
            try:
                ws_app.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.warning(f"[WS] run_forever error: {e}")
            if not rec["stop_event"].is_set():
                time.sleep(3)

    t = threading.Thread(target=_run, daemon=True)
    t.start()


# ── Main recording worker ─────────────────────────────────────────────────────
def _recording_worker(rec: dict) -> None:
    session_id  = rec["session_id"]
    amplinar_id = rec["amplinar_id"]
    room_name   = rec["room_name"]
    stop_event  = rec["stop_event"]

    if not LIVEKIT_API_KEY or not LIVEKIT_API_SECRET or not LIVEKIT_URL:
        rec["status"] = "error"
        rec["error"]  = "LIVEKIT_API_KEY / LIVEKIT_API_SECRET / LIVEKIT_URL not configured"
        return

    # segment_files: list of local paths (kept until successful completion)
    # segment_s3_urls: list of S3 URLs for each uploaded segment (for /retry)
    segment_files:    list = []
    segment_s3_urls:  list = []
    seg_index = 0

    rec["started_at_dt"] = datetime.now(timezone.utc)  # datetime object for duration calc
    rec["started_at"]    = rec["started_at_dt"].isoformat()
    rec["status"]        = "recording"
    rec["segment_s3_urls"] = segment_s3_urls  # expose for /status and /retry

    logger.info(f"[Recorder:{session_id}] Starting LiveKit capture for room={room_name}")

    _relay_ws_listener(rec)

    lk_proc = None
    lk_out  = None

    def _start_lk_segment():
        nonlocal lk_proc, lk_out
        tf = tempfile.NamedTemporaryFile(suffix="_lk.mp4", delete=False)
        tf.close()
        lk_out = tf.name
        lk_proc = _start_lk_subprocess(
            LIVEKIT_URL, room_name, lk_out,
            session_id=session_id, amplinar_id=amplinar_id
        )
        rec["lk_proc"] = lk_proc
        logger.info(f"[Recorder:{session_id}] LiveKit segment started → {lk_out}")

    def _stop_lk_segment() -> None:
        nonlocal lk_proc, lk_out, seg_index
        if lk_proc is None:
            return
        _stop_lk_subprocess(lk_proc)
        base = lk_out.replace('.mp4', '') if lk_out else ''
        seg_files = sorted(_glob.glob(f"{base}_seg*.mp4")) if base else []
        found = []
        def _has_streams(path: str) -> bool:
            """Return True if the file has at least one valid video or audio stream."""
            try:
                result = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "stream=codec_type",
                     "-of", "default=noprint_wrappers=1:nokey=1", path],
                    capture_output=True, timeout=10
                )
                output = result.stdout.decode().strip()
                return bool(output)  # non-empty means at least one stream found
            except Exception:
                return False

        if seg_files:
            for sf in seg_files:
                sz = os.path.getsize(sf)
                logger.info(f"[Recorder:{session_id}] Found segment: {sf} ({sz:,} bytes)")
                if sz > 0 and _has_streams(sf):
                    found.append(sf)
                elif sz > 0:
                    logger.warning(f"[Recorder:{session_id}] LiveKit segment has no streams — skipping: {sf}")
        else:
            sz = os.path.getsize(lk_out) if lk_out and os.path.exists(lk_out) else 0
            if sz > 0 and _has_streams(lk_out):
                found.append(lk_out)
            elif sz > 0:
                logger.warning(f"[Recorder:{session_id}] LiveKit segment has no streams — skipping: {lk_out}")
            else:
                logger.warning(f"[Recorder:{session_id}] LiveKit segment empty — skipping")

        for sf in found:
            segment_files.append(sf)
            # Upload immediately to S3 — segment is safe even if later steps fail
            s3_url = _upload_segment_to_s3(sf, session_id, amplinar_id, seg_index, "lk")
            if s3_url:
                segment_s3_urls.append(s3_url)
                # Register each real segment in the relay DB so it appears in the
                # recordings tab under the full recording for this session.
                # Use current_segment_title (updated on segment_change) for per-segment labels.
                _seg_label = rec.get("current_segment_title") or rec.get("amplinar_title", "")
                _notify_relay(session_id, amplinar_id, s3_url, title=_seg_label, recording_type='egress', scheduled_at=rec.get("scheduled_at"))
            seg_index += 1

        # Clean up placeholder only (not the actual segment files — those stay until success)
        if lk_out and lk_out not in found:
            try:
                os.unlink(lk_out)
            except Exception:
                pass
        lk_proc = None
        lk_out  = None
        rec["lk_proc"] = None

    try:
        _start_lk_segment()

        while not stop_event.is_set():
            triggered = threading.Event()

            def _wait():
                while not stop_event.is_set() and not rec["in_video_segment"].is_set():
                    stop_event.wait(timeout=1)
                    rec["in_video_segment"].wait(timeout=1)
                triggered.set()

            wait_t = threading.Thread(target=_wait, daemon=True)
            wait_t.start()
            triggered.wait()

            if stop_event.is_set():
                break

            if rec["in_video_segment"].is_set():
                logger.info(f"[Recorder:{session_id}] video_segment — pausing LiveKit capture")
                _stop_lk_segment()

                seg_info  = rec.get("pending_video_segment", {})
                seg_url   = seg_info.get("url", "")
                seg_title = seg_info.get("title", "")

                if seg_url:
                    try:
                        logger.info(f"[Recorder:{session_id}] Downloading video segment: {seg_url}")
                        resp = requests.get(seg_url, timeout=120, stream=True)
                        resp.raise_for_status()
                        tf = tempfile.NamedTemporaryFile(suffix="_video.mp4", delete=False)
                        for chunk in resp.iter_content(chunk_size=65536):
                            tf.write(chunk)
                        tf.close()
                        sz = os.path.getsize(tf.name)
                        logger.info(f"[Recorder:{session_id}] Video segment downloaded ({sz:,} bytes) → {tf.name}")
                        segment_files.append(tf.name)
                        # Upload video segment to S3 immediately
                        s3_url = _upload_segment_to_s3(tf.name, session_id, amplinar_id, seg_index, "vid")
                        if s3_url:
                            segment_s3_urls.append(s3_url)
                            # Register video segment in relay DB so it appears in recordings tab
                            _vid_label = seg_title or rec.get("current_segment_title") or rec.get("amplinar_title", "")
                            _notify_relay(session_id, amplinar_id, s3_url, title=_vid_label, recording_type='video', scheduled_at=rec.get("scheduled_at"))
                        seg_index += 1
                    except Exception as e:
                        logger.error(f"[Recorder:{session_id}] Failed to download video segment: {e}")

                logger.info(f"[Recorder:{session_id}] Waiting for video_segment_end...")
                rec["video_segment_ended"].clear()
                while not stop_event.is_set() and not rec["video_segment_ended"].is_set():
                    rec["video_segment_ended"].wait(timeout=1)

                if stop_event.is_set():
                    break

                logger.info(f"[Recorder:{session_id}] video_segment_end — resuming LiveKit capture")
                rec["in_video_segment"].clear()
                _start_lk_segment()

        # Stop the final LiveKit segment
        _stop_lk_segment()

        # ── Post-recording: concat + upload ───────────────────────────────────
        valid_segments = [f for f in segment_files if os.path.exists(f) and os.path.getsize(f) > 0]

        # v10+ of livekit_recorder.js uploads segments to S3 and deletes local files
        # before recorder.py can collect them. If no local files remain, fall back to
        # the S3 URLs that were registered via /segment-complete callbacks.
        _s3_fallback_tmp: list = []
        if not valid_segments and segment_s3_urls:
            logger.info(f"[Recorder:{session_id}] No local segments — downloading {len(segment_s3_urls)} S3 segment(s) for stitching")
            try:
                s3 = _s3_client()
                for i, s3url in enumerate(segment_s3_urls):
                    key = s3url.split(f"{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/")[-1]
                    tf = tempfile.NamedTemporaryFile(suffix=f"_s3dl_{i:03d}.mp4", delete=False)
                    tf.close()
                    logger.info(f"[Recorder:{session_id}] Downloading S3 segment {i}: {key}")
                    s3.download_file(S3_BUCKET_NAME, key, tf.name)
                    sz = os.path.getsize(tf.name)
                    logger.info(f"[Recorder:{session_id}] Downloaded {sz:,} bytes -> {tf.name}")
                    _s3_fallback_tmp.append(tf.name)
                valid_segments = [f for f in _s3_fallback_tmp if os.path.getsize(f) > 0]
            except Exception as _s3dl_err:
                logger.error(f"[Recorder:{session_id}] S3 fallback download failed: {_s3dl_err}")

        if not valid_segments:
            raise RuntimeError("No valid segment files to stitch")

        logger.info(f"[Recorder:{session_id}] Stitching {len(valid_segments)} segment(s)")
        rec["status"] = "concatenating"

        # Write final MP4 to a temp file (stream-to-disk, no OOM)
        with tempfile.NamedTemporaryFile(suffix="_final.mp4", delete=False) as outf:
            final_path = outf.name

        try:
            _concat_segments_to_file(valid_segments, final_path)

            rec["status"] = "uploading"
            now    = datetime.now(timezone.utc)
            s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_{session_id}.mp4"
            url    = upload_file_to_s3(final_path, s3_key)
        finally:
            try:
                os.unlink(final_path)
            except Exception:
                pass

        rec["recording_url"] = url
        rec["status"]        = "complete"
        rec["completed_at"]  = now.isoformat()
        logger.info(f"[Recorder:{session_id}] Complete: {url}")

        # Duration calculation — use datetime objects, not ISO strings
        started_dt = rec.get("started_at_dt")
        duration_secs = int((now - started_dt).total_seconds()) if started_dt else 0
        _notify_relay(session_id, amplinar_id, url,
                      title=rec.get("amplinar_title", ""), duration_seconds=duration_secs,
                      scheduled_at=rec.get("scheduled_at"))

        # Clean up local segment files only after successful completion
        for f in segment_files:
            try:
                os.unlink(f)
            except Exception:
                pass
        # Clean up S3 fallback temp files (downloaded for stitching)
        for f in _s3_fallback_tmp:
            try:
                os.unlink(f)
            except Exception:
                pass

    except Exception as e:
        logger.error(f"[Recorder:{session_id}] Failed: {e}")
        rec["status"] = "error"
        rec["error"]  = str(e)
        # Kill subprocess if still running
        if lk_proc and lk_proc.poll() is None:
            lk_proc.kill()
        # DO NOT delete segment files on failure — they stay on disk AND are already
        # uploaded to S3 incrementally. Use /retry to re-stitch from S3 URLs.
        logger.info(f"[Recorder:{session_id}] {len(segment_s3_urls)} segment(s) already on S3: {segment_s3_urls}")
    finally:
        ws = rec.get("_ws")
        if ws:
            try:
                ws.close()
            except Exception:
                pass


# ── Segment-complete callback (called by livekit_recorder.js) ────────────────
@app.route("/segment-complete", methods=["POST"])
def segment_complete():
    if RECORDER_API_KEY and request.headers.get("X-Recorder-Key") != RECORDER_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data        = request.get_json() or {}
    session_id  = data.get("session_id", "unknown")
    amplinar_id = data.get("amplinar_id", "unknown")
    seg_idx     = data.get("segment_index", 0)
    s3_url      = data.get("audio_path", "") or data.get("video_path", "")

    # Get active recording FIRST before using rec (fixes NameError)
    with _recording_lock:
        active_rec = _recording

    if s3_url and s3_url.startswith("https://"):
        logger.info(f"[Segment] Segment {seg_idx} already in S3: {s3_url}")
        if active_rec and active_rec.get("session_id") == session_id:
            if "segment_s3_urls" not in active_rec:
                active_rec["segment_s3_urls"] = []
            active_rec["segment_s3_urls"].append(s3_url)
        # Only register real segments in the relay DB — skip tiny placeholder files
        # (livekit_recorder.js creates small _seg001.mp4 placeholders that are not
        # real recordings; real segments are in the /segments/ subfolder with _lk_ or _vid_ in the name)
        is_real_segment = '/segments/' in s3_url and ('_lk_' in s3_url or '_vid_' in s3_url)
        if is_real_segment:
            _seg_cb_label = (active_rec.get("current_segment_title") or active_rec.get("amplinar_title", "")) if active_rec else ""
            _notify_relay(session_id, amplinar_id, s3_url, title=_seg_cb_label,
                          scheduled_at=active_rec.get("scheduled_at") if active_rec else None)
        else:
            logger.info(f"[Segment] Skipping relay notify for placeholder file: {s3_url.split('/')[-1]}")
        return jsonify({"status": "ok", "segment_index": seg_idx, "url": s3_url})

    # Legacy fallback: local file path
    video_path = data.get("video_path", "")
    if not video_path or not os.path.exists(video_path):
        logger.warning(f"[Segment] Segment {seg_idx}: no S3 URL and no local file found")
        return jsonify({"error": "no url or file"}), 404

    sz = os.path.getsize(video_path)
    logger.info(f"[Segment] Uploading segment {seg_idx} locally ({sz:,} bytes) for session {session_id}")

    def _upload():
        try:
            now    = datetime.now(timezone.utc)
            s3_key = (f"amplinar-recordings/{amplinar_id}/segments/"
                      f"{session_id}_cb_{seg_idx:03d}_{now.strftime('%H%M%S')}.mp4")
            url = upload_file_to_s3(video_path, s3_key)
            logger.info(f"[Segment] Segment {seg_idx} uploaded: {url}")
            if active_rec and active_rec.get("session_id") == session_id:
                if "segment_s3_urls" not in active_rec:
                    active_rec["segment_s3_urls"] = []
                active_rec["segment_s3_urls"].append(url)
            _seg_cb_label2 = (active_rec.get("current_segment_title") or active_rec.get("amplinar_title", "")) if active_rec else ""
            _notify_relay(session_id, amplinar_id, url, title=_seg_cb_label2,
                          scheduled_at=active_rec.get("scheduled_at") if active_rec else None)
        except Exception as e:
            logger.error(f"[Segment] Upload failed for segment {seg_idx}: {e}")
            # DO NOT delete on failure
        else:
            try:
                os.unlink(video_path)
            except Exception:
                pass

    threading.Thread(target=_upload, daemon=True).start()
    return jsonify({"status": "uploading", "segment_index": seg_idx})


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "amplinar-recorder", "mode": "livekit-direct"})


@app.route("/start", methods=["POST"])
def start_recording():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data           = request.get_json() or {}
    session_id     = data.get("session_id")
    amplinar_id    = data.get("amplinar_id")
    amplinar_title = data.get("amplinar_title") or amplinar_id or ""
    scheduled_at   = data.get("scheduled_at")
    room_name      = data.get("room_name") or session_id
    if not session_id or not amplinar_id:
        return jsonify({"error": "session_id and amplinar_id are required"}), 400

    with _recording_lock:
        global _recording
        if _recording and _recording.get("status") in ("recording", "starting"):
            return jsonify({
                "error": "A recording is already in progress",
                "session_id": _recording["session_id"],
            }), 409
        rec = {
            "session_id":            session_id,
            "amplinar_id":           amplinar_id,
            "amplinar_title":        amplinar_title,
            "scheduled_at":          scheduled_at,
            "current_segment_title": amplinar_title,  # updated on each segment_change
            "room_name":             room_name,
            "status":                "starting",
            "stop_event":            threading.Event(),
            "in_video_segment":      threading.Event(),
            "video_segment_ended":   threading.Event(),
            "pending_video_segment": None,
            "lk_proc":               None,
            "started_at":            None,
            "started_at_dt":         None,
            "completed_at":          None,
            "recording_url":         None,
            "segment_s3_urls":       [],
            "error":                 None,
            "_ws":                   None,
        }
        _recording = rec

    threading.Thread(target=_recording_worker, args=(rec,), daemon=True).start()
    logger.info(f"[Recorder] Started session={session_id} room={room_name} amplinar={amplinar_id}")
    return jsonify({"status": "started", "session_id": session_id})


@app.route("/stop", methods=["POST"])
def stop_recording():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data       = request.get_json() or {}
    session_id = data.get("session_id")

    with _recording_lock:
        rec = _recording

    if not rec:
        return jsonify({"error": "No active recording"}), 404

    if session_id and rec["session_id"] != session_id:
        return jsonify({
            "error":      f"Active recording is for session {rec['session_id']}, not {session_id}",
            "active_session_id": rec["session_id"],
        }), 409

    if rec["status"] not in ("recording", "starting"):
        return jsonify({"status": rec["status"], "session_id": rec["session_id"]})

    logger.info(f"[Recorder] Stop requested for {rec['session_id']}")
    rec["stop_event"].set()
    return jsonify({"status": "stopping", "session_id": rec["session_id"]})


@app.route("/retry", methods=["POST"])
def retry_recording():
    """Re-stitch a failed recording from S3 segment URLs.

    Body: {
      "session_id":    "agent-...",
      "amplinar_id":   "2050",
      "segment_urls":  ["https://...", ...]   # optional — uses last recording's URLs if omitted
    }
    """
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data        = request.get_json() or {}
    session_id  = data.get("session_id")
    amplinar_id = data.get("amplinar_id")

    # Use provided URLs or fall back to last recording's segment URLs
    segment_urls = data.get("segment_urls")
    if not segment_urls:
        with _recording_lock:
            rec = _recording
        if rec:
            segment_urls = rec.get("segment_s3_urls", [])
            if not amplinar_id:
                amplinar_id = rec.get("amplinar_id")
            if not session_id:
                session_id = rec.get("session_id")

    if not segment_urls:
        return jsonify({"error": "No segment_urls provided and no previous recording found"}), 400
    if not session_id or not amplinar_id:
        return jsonify({"error": "session_id and amplinar_id required"}), 400

    def _do_retry():
        logger.info(f"[Retry] Re-stitching {len(segment_urls)} segment(s) for {session_id}")
        tmp_files = []
        try:
            # Download all S3 segments to temp files
            s3 = _s3_client()
            for i, url in enumerate(segment_urls):
                # Parse S3 key from URL
                key = url.split(f"{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/")[-1]
                tf = tempfile.NamedTemporaryFile(suffix=f"_retry_{i:03d}.mp4", delete=False)
                tf.close()
                logger.info(f"[Retry] Downloading segment {i}: {key}")
                s3.download_file(S3_BUCKET_NAME, key, tf.name)
                sz = os.path.getsize(tf.name)
                logger.info(f"[Retry] Downloaded {sz:,} bytes → {tf.name}")
                tmp_files.append(tf.name)

            valid = [f for f in tmp_files if os.path.getsize(f) > 0]
            if not valid:
                raise RuntimeError("All downloaded segments are empty")

            with tempfile.NamedTemporaryFile(suffix="_retry_final.mp4", delete=False) as outf:
                final_path = outf.name
            tmp_files.append(final_path)

            _concat_segments_to_file(valid, final_path)

            now    = datetime.now(timezone.utc)
            s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_{session_id}_retry.mp4"
            url    = upload_file_to_s3(final_path, s3_key)

            logger.info(f"[Retry] Complete: {url}")
            _notify_relay(session_id, amplinar_id, url, title="Recovered Recording")

            with _recording_lock:
                rec = _recording
            if rec and rec.get("session_id") == session_id:
                rec["recording_url"] = url
                rec["status"]        = "complete"

        except Exception as e:
            logger.error(f"[Retry] Failed: {e}")
        finally:
            for f in tmp_files:
                try:
                    os.unlink(f)
                except Exception:
                    pass

    threading.Thread(target=_do_retry, daemon=True).start()
    return jsonify({"status": "retrying", "session_id": session_id, "segments": len(segment_urls)})


@app.route("/status")
def get_status():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    with _recording_lock:
        rec = _recording

    if not rec:
        return jsonify({"status": "idle"})

    return jsonify({
        "status":          rec["status"],
        "session_id":      rec["session_id"],
        "amplinar_id":     rec["amplinar_id"],
        "started_at":      rec.get("started_at"),
        "completed_at":    rec.get("completed_at"),
        "recording_url":   rec.get("recording_url"),
        "segment_s3_urls": rec.get("segment_s3_urls", []),
        "error":           rec.get("error"),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
