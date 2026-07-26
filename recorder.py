"""
Amplinar Recorder Service — LiveKit Direct Capture
===================================================
Records a live Amplinar session by connecting directly to the LiveKit room
and capturing avatar video + audio tracks. Video segments (played locally
in viewer browsers) are downloaded by URL and spliced in at the correct
position. All segments are stitched with FFmpeg into one final WebM file
and uploaded to S3.

Architecture
------------
- On /start: generate a LiveKit access token for the recorder participant,
  spawn livekit_recorder.js to capture the room's audio+video tracks
- Listen to the relay WebSocket for segment events:
    video_segment      → pause LiveKit capture, download the video file
    video_segment_end  → resume LiveKit capture
- On /stop: SIGTERM the Node.js worker, stitch all segments, upload to S3

API
---
  POST /start   { "session_id": "...", "amplinar_id": "...", "room_name": "..." }
  POST /stop    { "session_id": "..." }
  GET  /status
  GET  /health
"""
from __future__ import annotations

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
RELAY_API_KEY        = os.environ.get("RECORDER_API_KEY", "")  # use RECORDER_API_KEY for callback auth

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
    """Generate a LiveKit access token for the recorder participant."""
    now = int(time.time())
    payload = {
        "iss": LIVEKIT_API_KEY,
        "sub": identity,
        "iat": now,
        "exp": now + 7200,  # 2 hours
        "video": {
            "room": room_name,
            "roomJoin": True,
            "canPublish": False,
            "canSubscribe": True,
            "canPublishData": False,
        },
    }
    return jwt.encode(payload, LIVEKIT_API_SECRET, algorithm="HS256")


# ── S3 upload ─────────────────────────────────────────────────────────────────
def upload_to_s3(data: bytes, s3_key: str) -> str:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
    )
    logger.info(f"[S3] Uploading {len(data)} bytes → {s3_key}")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=data, ContentType="video/webm")
    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
    logger.info(f"[S3] Done: {url}")
    return url


# ── Relay notification ────────────────────────────────────────────────────────
def _notify_relay(session_id: str, amplinar_id: str, recording_url: str) -> None:
    if not RELAY_URL:
        return
    try:
        resp = requests.post(
            f"{RELAY_URL}/api/session/recording-complete",
            json={
                "session_id":  session_id,
                "amplinar_id": amplinar_id,
                "s3_url":      recording_url,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            },
            headers={"x-api-key": RELAY_API_KEY},
            timeout=10,
        )
        logger.info(f"[Recorder] Relay notified: {resp.status_code}")
    except Exception as e:
        logger.error(f"[Recorder] Relay notification failed: {e}")


# ── FFmpeg concat ─────────────────────────────────────────────────────────────
def _concat_segments(segment_paths: list) -> bytes:
    """Concatenate a list of WebM/MP4 segment files into one WebM."""
    if len(segment_paths) == 1:
        with open(segment_paths[0], "rb") as f:
            return f.read()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as listf:
        for p in segment_paths:
            listf.write(f"file '{p}'\n")
        list_path = listf.name

    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as outf:
        out_path = outf.name

    try:
        result = subprocess.run(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_path,
             "-c:v", "libvpx", "-b:v", "1500k", "-deadline", "realtime", "-cpu-used", "8",
             "-c:a", "libopus", "-b:a", "128k",
             out_path],
            capture_output=True,
        )
        if result.returncode != 0:
            logger.error(f"[FFmpeg] concat failed: {result.stderr.decode()[-500:]}")
            raise RuntimeError("FFmpeg concat failed")
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        try:
            os.unlink(list_path)
        except Exception:
            pass
        try:
            os.unlink(out_path)
        except Exception:
            pass


# ── LiveKit recorder subprocess ───────────────────────────────────────────────
SEGMENT_MINUTES = int(os.environ.get("RECORDER_SEGMENT_MINUTES", "5"))


def _start_lk_subprocess(lk_url: str, room_name: str, output_path: str,
                         session_id: str = "", amplinar_id: str = "") -> subprocess.Popen:
    """Spawn livekit_recorder.js and return the Popen object.
    Starts a background thread that streams stdout live so logs appear
    in Railway in real time rather than only after SIGTERM.
    """
    port = int(os.environ.get("PORT", 8080))
    callback_url = f"http://localhost:{port}/segment-complete"
    env = os.environ.copy()
    proc = subprocess.Popen(
        [
            "node", LK_RECORDER_JS,
            lk_url, room_name, output_path,
            f"--segment-minutes={SEGMENT_MINUTES}",
            f"--callback-url={callback_url}",
            f"--callback-key={RECORDER_API_KEY}",
            f"--session-id={session_id}",
            f"--amplinar-id={amplinar_id}",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered
    )
    logger.info(f"[Recorder] livekit_recorder.js started (pid={proc.pid})")

    # Stream stdout live in a background thread so logs appear immediately
    def _live_stream():
        try:
            for line in proc.stdout:
                logger.info(f"[lk-rec] {line.rstrip()}")
        except Exception:
            pass

    t = threading.Thread(target=_live_stream, daemon=True)
    t.start()
    # Store thread on proc so _stop_lk_subprocess can join it
    proc._stdout_thread = t

    return proc


def _stop_lk_subprocess(proc: subprocess.Popen, timeout: int = 120) -> bool:
    """Send SIGTERM to the recorder subprocess and wait for it to finish."""
    if proc.poll() is not None:
        return True  # already done
    try:
        proc.send_signal(signal.SIGTERM)
        logger.info(f"[Recorder] SIGTERM sent to pid={proc.pid}")
    except Exception as e:
        logger.warning(f"[Recorder] SIGTERM failed: {e}")

    try:
        proc.wait(timeout=timeout)
        # Join the live-stream thread so all output is flushed before we return
        t = getattr(proc, '_stdout_thread', None)
        if t:
            t.join(timeout=10)
        return proc.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error(f"[Recorder] Subprocess did not exit in {timeout}s — killing")
        proc.kill()
        return False


# ── Relay WebSocket listener ──────────────────────────────────────────────────
def _relay_ws_listener(rec: dict) -> None:
    """
    Connects to the relay WebSocket and listens for video_segment events.
    When a video_segment fires, pauses the LiveKit capture and downloads
    the video file. When video_segment_end fires, resumes LiveKit capture.
    """
    import json

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
            url = msg.get("url", "")
            title = msg.get("title", "")
            logger.info(f"[WS] video_segment: {title} — {url}")
            if url:
                rec["pending_video_segment"] = {"url": url, "title": title}
                rec["in_video_segment"].set()

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

    # Run until stop_event is set
    def _run():
        while not rec["stop_event"].is_set():
            try:
                ws_app.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.warning(f"[WS] run_forever error: {e}")
            if not rec["stop_event"].is_set():
                time.sleep(3)  # reconnect delay

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

    segment_files: list = []   # ordered list of (path, is_temp) tuples
    rec["started_at"] = datetime.now(timezone.utc).isoformat()
    rec["status"]     = "recording"

    logger.info(f"[Recorder:{session_id}] Starting LiveKit capture for room={room_name}")

    # Start relay WebSocket listener
    _relay_ws_listener(rec)

    # ── Segment loop ──────────────────────────────────────────────────────────
    # We alternate between LiveKit capture segments and downloaded video segments.
    # The loop runs until stop_event is set.

    lk_proc = None
    lk_out  = None

    def _start_lk_segment():
        nonlocal lk_proc, lk_out
        tf = tempfile.NamedTemporaryFile(suffix="_lk.webm", delete=False)
        tf.close()
        lk_out = tf.name
        lk_proc = _start_lk_subprocess(
            LIVEKIT_URL, room_name, lk_out,
            session_id=session_id, amplinar_id=amplinar_id
        )
        logger.info(f"[Recorder:{session_id}] LiveKit segment started → {lk_out}")

    def _stop_lk_segment():
        nonlocal lk_proc, lk_out
        if lk_proc is None:
            return
        ok = _stop_lk_subprocess(lk_proc)
        sz = os.path.getsize(lk_out) if lk_out and os.path.exists(lk_out) else 0
        logger.info(f"[Recorder:{session_id}] LiveKit segment done (ok={ok}, {sz} bytes) → {lk_out}")
        if sz > 0:
            segment_files.append(lk_out)
        else:
            try:
                os.unlink(lk_out)
            except Exception:
                pass
        lk_proc = None
        lk_out  = None

    try:
        # Start first LiveKit segment
        _start_lk_segment()

        while not stop_event.is_set():
            # Wait for either: stop_event OR a video_segment event
            triggered = threading.Event()

            def _wait():
                # Wait for stop or video_segment
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
                # Stop the current LiveKit segment
                logger.info(f"[Recorder:{session_id}] video_segment event — pausing LiveKit capture")
                _stop_lk_segment()

                # Download the video segment
                seg_info = rec.get("pending_video_segment", {})
                seg_url  = seg_info.get("url", "")
                seg_title = seg_info.get("title", "")

                if seg_url:
                    try:
                        logger.info(f"[Recorder:{session_id}] Downloading video segment: {seg_url}")
                        resp = requests.get(seg_url, timeout=60, stream=True)
                        resp.raise_for_status()
                        tf = tempfile.NamedTemporaryFile(suffix="_video.mp4", delete=False)
                        for chunk in resp.iter_content(chunk_size=65536):
                            tf.write(chunk)
                        tf.close()
                        sz = os.path.getsize(tf.name)
                        logger.info(f"[Recorder:{session_id}] Video segment downloaded ({sz} bytes) → {tf.name}")
                        segment_files.append(tf.name)
                    except Exception as e:
                        logger.error(f"[Recorder:{session_id}] Failed to download video segment: {e}")

                # Wait for video_segment_end or stop
                logger.info(f"[Recorder:{session_id}] Waiting for video_segment_end...")
                rec["video_segment_ended"].clear()
                while not stop_event.is_set() and not rec["video_segment_ended"].is_set():
                    rec["video_segment_ended"].wait(timeout=1)

                if stop_event.is_set():
                    break

                # Resume LiveKit capture
                logger.info(f"[Recorder:{session_id}] video_segment_end — resuming LiveKit capture")
                rec["in_video_segment"].clear()
                _start_lk_segment()

        # Stop the final LiveKit segment
        _stop_lk_segment()

        # ── Post-recording: concat + upload ───────────────────────────────────
        valid_segments = [f for f in segment_files if os.path.exists(f) and os.path.getsize(f) > 0]
        if not valid_segments:
            raise RuntimeError("No valid segment files to upload")

        logger.info(f"[Recorder:{session_id}] Concatenating {len(valid_segments)} segment(s)")
        rec["status"] = "concatenating"
        final_bytes = _concat_segments(valid_segments)

        rec["status"] = "uploading"
        now    = datetime.now(timezone.utc)
        s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_{session_id}.webm"
        url    = upload_to_s3(final_bytes, s3_key)

        rec["recording_url"] = url
        rec["status"]        = "complete"
        rec["completed_at"]  = now.isoformat()
        logger.info(f"[Recorder:{session_id}] Complete: {url}")
        _notify_relay(session_id, amplinar_id, url)

    except Exception as e:
        logger.error(f"[Recorder:{session_id}] Failed: {e}")
        rec["status"] = "error"
        rec["error"]  = str(e)
        # Make sure subprocess is killed
        if lk_proc and lk_proc.poll() is None:
            lk_proc.kill()
    finally:
        # Clean up segment files
        for f in segment_files:
            try:
                os.unlink(f)
            except Exception:
                pass
        # Close WebSocket
        ws = rec.get("_ws")
        if ws:
            try:
                ws.close()
            except Exception:
                pass


# ── Segment-complete callback (called by livekit_recorder.js) ────────────────
@app.route("/segment-complete", methods=["POST"])
def segment_complete():
    """Called by livekit_recorder.js when a rolling segment is ready.
    Uploads the merged segment WebM to S3 immediately.
    """
    if RECORDER_API_KEY and request.headers.get("X-Recorder-Key") != RECORDER_API_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    data        = request.get_json() or {}
    session_id  = data.get("session_id", "unknown")
    amplinar_id = data.get("amplinar_id", "unknown")
    seg_idx     = data.get("segment_index", 0)
    video_path  = data.get("video_path", "")

    if not video_path or not os.path.exists(video_path):
        logger.warning(f"[Segment] Segment {seg_idx} path not found: {video_path}")
        return jsonify({"error": "file not found"}), 404

    sz = os.path.getsize(video_path)
    logger.info(f"[Segment] Uploading segment {seg_idx} ({sz} bytes) for session {session_id}")

    def _upload():
        try:
            with open(video_path, "rb") as f:
                data_bytes = f.read()
            now    = datetime.now(timezone.utc)
            s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_seg{seg_idx:03d}_{session_id}.webm"
            url    = upload_to_s3(data_bytes, s3_key)
            logger.info(f"[Segment] Segment {seg_idx} uploaded: {url}")
            # Notify relay of partial recording
            _notify_relay(session_id, amplinar_id, url)
        except Exception as e:
            logger.error(f"[Segment] Upload failed for segment {seg_idx}: {e}")
        finally:
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

    data        = request.get_json() or {}
    session_id  = data.get("session_id")
    amplinar_id = data.get("amplinar_id")
    room_name   = data.get("room_name") or session_id  # room_name defaults to session_id

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
            "session_id":         session_id,
            "amplinar_id":        amplinar_id,
            "room_name":          room_name,
            "status":             "starting",
            "stop_event":         threading.Event(),
            "in_video_segment":   threading.Event(),
            "video_segment_ended": threading.Event(),
            "pending_video_segment": None,
            "started_at":         None,
            "completed_at":       None,
            "recording_url":      None,
            "error":              None,
            "_ws":                None,
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
            "error": f"Active recording is for session {rec['session_id']}, not {session_id}",
        }), 409

    if rec["status"] not in ("recording", "starting"):
        return jsonify({"status": rec["status"], "session_id": rec["session_id"]})

    logger.info(f"[Recorder] Stop requested for {rec['session_id']}")
    rec["stop_event"].set()

    return jsonify({"status": "stopping", "session_id": rec["session_id"]})


@app.route("/status")
def get_status():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    with _recording_lock:
        rec = _recording

    if not rec:
        return jsonify({"status": "idle"})

    return jsonify({
        "status":        rec["status"],
        "session_id":    rec["session_id"],
        "amplinar_id":   rec["amplinar_id"],
        "started_at":    rec.get("started_at"),
        "completed_at":  rec.get("completed_at"),
        "recording_url": rec.get("recording_url"),
        "error":         rec.get("error"),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
