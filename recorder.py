"""
Amplinar Recorder Service
=========================
A lightweight Flask service that records a live Amplinar session as a single
MP4 by running a headless Chromium browser pointed at viewer.html, capturing
the virtual display with FFmpeg, and uploading the result to S3.

API:
  POST /start   { "viewer_url": "...", "session_id": "...", "amplinar_id": "..." }
  POST /stop    { "session_id": "..." }
  GET  /status  { "session_id": "..." }
  GET  /health

Environment variables:
  VIEWER_BASE_URL        Base URL of the relay viewer (e.g. https://amplinar-relay-production.up.railway.app)
  RECORDER_API_KEY       Shared secret — relay must send this in X-Recorder-Key header
  S3_ACCESS_KEY_ID       AWS access key
  S3_SECRET_ACCESS_KEY   AWS secret key
  S3_BUCKET_NAME         S3 bucket name (wholesalehotelrates-images)
  S3_REGION              AWS region (default: us-east-1)
  RELAY_URL              Relay base URL for posting recording_complete notification
  RELAY_API_KEY          Relay API key for the notification callback
"""
from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("amplinar-recorder")

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
RECORDER_API_KEY = os.environ.get("RECORDER_API_KEY", "")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "wholesalehotelrates-images")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
RELAY_URL = os.environ.get("RELAY_URL", "")
RELAY_API_KEY = os.environ.get("RELAY_API_KEY", "")

# Virtual display dimensions
DISPLAY_WIDTH = 1280
DISPLAY_HEIGHT = 720
DISPLAY_NUM = 99  # Xvfb display number

# ── State ─────────────────────────────────────────────────────────────────────
# One active recording at a time (Amplinar runs one session at a time)
_recording: Optional[dict] = None
_recording_lock = threading.Lock()


# ── Auth helper ───────────────────────────────────────────────────────────────
def check_auth() -> bool:
    if not RECORDER_API_KEY:
        return True  # No key configured — allow all (dev mode)
    return request.headers.get("X-Recorder-Key") == RECORDER_API_KEY


# ── S3 upload ─────────────────────────────────────────────────────────────────
def upload_to_s3(local_path: str, s3_key: str) -> str:
    """Upload a file to S3 and return the public URL."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
    )
    logger.info(f"[S3] Uploading {local_path} → s3://{S3_BUCKET_NAME}/{s3_key}")
    s3.upload_file(
        local_path,
        S3_BUCKET_NAME,
        s3_key,
        ExtraArgs={"ContentType": "video/mp4"},
    )
    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
    logger.info(f"[S3] Upload complete: {url}")
    return url


# ── Recording worker ──────────────────────────────────────────────────────────
def _recording_worker(rec: dict) -> None:
    """
    Runs in a background thread:
    1. Start Xvfb virtual display
    2. Start Chromium pointing at viewer_url
    3. Start FFmpeg capturing the display to an MP4 file
    Blocks until stop() is called (sets rec['stop_event']).
    Then finalizes FFmpeg, uploads to S3, notifies relay.
    """
    session_id = rec["session_id"]
    amplinar_id = rec["amplinar_id"]
    viewer_url = rec["viewer_url"]
    output_path = f"/tmp/recording_{session_id}.mp4"
    rec["output_path"] = output_path

    display = f":{DISPLAY_NUM}"
    env = {**os.environ, "DISPLAY": display}

    xvfb_proc = None
    chromium_proc = None
    ffmpeg_proc = None

    try:
        # 1. Start Xvfb
        logger.info(f"[Recorder:{session_id}] Starting Xvfb on {display}")
        xvfb_proc = subprocess.Popen(
            ["Xvfb", display, "-screen", "0", f"{DISPLAY_WIDTH}x{DISPLAY_HEIGHT}x24", "-ac"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        rec["xvfb_proc"] = xvfb_proc
        time.sleep(1.5)  # Give Xvfb time to start

        # 1b. Start PulseAudio in --system mode (works in headless Docker as root)
        pa_ok = False
        try:
            # Clean up any stale PulseAudio state from previous runs
            subprocess.run(["rm", "-rf", "/var/run/pulse", "/var/lib/pulse", "/root/.config/pulse/cookie"],
                           capture_output=True)
            # Start PulseAudio WITHOUT -D so we can track the process
            pa_proc = subprocess.Popen(
                ["pulseaudio", "--verbose", "--exit-idle-time=-1", "--system", "--disallow-exit"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            time.sleep(2)  # Give PulseAudio time to start
            if pa_proc.poll() is not None:
                pa_err_out = pa_proc.stderr.read().decode(errors='replace')
                raise RuntimeError(f"PulseAudio exited immediately: {pa_err_out[-300:]}")
            rec["pa_proc"] = pa_proc
            # Load null sink so Chromium audio routes through PulseAudio
            env["PULSE_SERVER"] = "unix:/var/run/pulse/native"
            r1 = subprocess.run(["pactl", "load-module", "module-null-sink", "sink_name=loopback"],
                                env=env, capture_output=True, timeout=5)
            r2 = subprocess.run(["pactl", "set-default-sink", "loopback"],
                                env=env, capture_output=True, timeout=5)
            r3 = subprocess.run(["pactl", "set-default-source", "loopback.monitor"],
                                env=env, capture_output=True, timeout=5)
            logger.info(f"[Recorder:{session_id}] PulseAudio ready (sink={r1.returncode}, default-sink={r2.returncode}, default-src={r3.returncode})")
            if any(r.returncode != 0 for r in [r1, r2, r3]):
                for r, name in [(r1,'null-sink'),(r2,'default-sink'),(r3,'default-src')]:
                    if r.returncode != 0:
                        logger.warning(f"[Recorder:{session_id}] pactl {name}: {r.stderr.decode(errors='replace').strip()}")
                raise RuntimeError("pactl setup failed — see warnings above")
            pa_ok = True
            logger.info(f"[Recorder:{session_id}] PulseAudio audio capture ready")
        except Exception as pa_err:
            logger.warning(f"[Recorder:{session_id}] PulseAudio failed ({pa_err}) — falling back to silent audio")
            env.pop("PULSE_SERVER", None)

        # 2. Start Chromium (headless=false so it renders to the virtual display)
        logger.info(f"[Recorder:{session_id}] Starting Chromium → {viewer_url}")
        chromium_proc = subprocess.Popen(
            [
                "chromium",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1280,720",
                "--window-position=0,0",
                "--autoplay-policy=no-user-gesture-required",
                "--disable-web-security",
                "--allow-running-insecure-content",
                f"--display={display}",
                viewer_url,
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        rec["chromium_proc"] = chromium_proc
        time.sleep(3)  # Give Chromium time to load the page

        # 3. Start FFmpeg capturing the virtual display
        logger.info(f"[Recorder:{session_id}] Starting FFmpeg capture → {output_path} (audio={'pulse' if pa_ok else 'silent'})")
        audio_args = ["-f", "pulse", "-i", "loopback.monitor"] if pa_ok else ["-f", "lavfi", "-i", "anullsrc=r=44100:cl=stereo"]
        ffmpeg_proc = subprocess.Popen(
            [
                "ffmpeg",
                "-y",
                "-f", "x11grab",
                "-r", "30",
                "-s", f"{DISPLAY_WIDTH}x{DISPLAY_HEIGHT}",
                "-i", display,
                *audio_args,
                "-c:v", "libx264",
                "-preset", "ultrafast",
                "-crf", "23",
                "-c:a", "aac",
                "-b:a", "128k",
                "-movflags", "+faststart",
                output_path,
            ],
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        # Give FFmpeg a moment to open inputs and confirm it started
        time.sleep(2)
        if ffmpeg_proc.poll() is not None:
            stderr_out = ffmpeg_proc.stderr.read().decode(errors='replace')
            raise RuntimeError(f"FFmpeg exited immediately: {stderr_out[-500:]}")
        rec["ffmpeg_proc"] = ffmpeg_proc
        rec["started_at"] = datetime.now(timezone.utc).isoformat()
        rec["status"] = "recording"
        logger.info(f"[Recorder:{session_id}] Recording started")

        # Wait for stop signal
        rec["stop_event"].wait()
        logger.info(f"[Recorder:{session_id}] Stop signal received — finalizing")

    except Exception as e:
        logger.error(f"[Recorder:{session_id}] Error during recording setup: {e}")
        rec["status"] = "error"
        rec["error"] = str(e)
    finally:
        # Stop FFmpeg gracefully: send 'q\n' to stdin so it finalizes the MP4
        if ffmpeg_proc and ffmpeg_proc.poll() is None:
            try:
                ffmpeg_proc.stdin.write(b"q\n")
                ffmpeg_proc.stdin.flush()
                ffmpeg_proc.stdin.close()
                ffmpeg_proc.wait(timeout=30)
                logger.info(f"[Recorder:{session_id}] FFmpeg finalized cleanly")
            except Exception as e:
                logger.warning(f"[Recorder:{session_id}] FFmpeg graceful stop failed ({e}), sending SIGINT")
                try:
                    import signal as _signal
                    ffmpeg_proc.send_signal(_signal.SIGINT)
                    ffmpeg_proc.wait(timeout=15)
                except Exception:
                    try:
                        ffmpeg_proc.kill()
                    except Exception:
                        pass
        elif ffmpeg_proc:
            # FFmpeg already exited — log stderr for diagnosis
            try:
                stderr_out = ffmpeg_proc.stderr.read().decode(errors='replace')
                if stderr_out:
                    logger.warning(f"[Recorder:{session_id}] FFmpeg stderr: {stderr_out[-500:]}")
            except Exception:
                pass

        # Stop Chromium
        if chromium_proc and chromium_proc.poll() is None:
            try:
                chromium_proc.terminate()
                chromium_proc.wait(timeout=5)
            except Exception:
                try:
                    chromium_proc.kill()
                except Exception:
                    pass

        # Stop PulseAudio
        pa_proc = rec.get("pa_proc")
        if pa_proc and pa_proc.poll() is None:
            try:
                pa_proc.terminate()
                pa_proc.wait(timeout=5)
            except Exception:
                try:
                    pa_proc.kill()
                except Exception:
                    pass

        # Stop Xvfb
        if xvfb_proc and xvfb_proc.poll() is None:
            try:
                xvfb_proc.terminate()
                xvfb_proc.wait(timeout=5)
            except Exception:
                try:
                    xvfb_proc.kill()
                except Exception:
                    pass

        # Upload to S3 if recording file exists
        if rec.get("status") == "recording" and os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            try:
                rec["status"] = "uploading"
                now = datetime.now(timezone.utc)
                s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_{session_id}.mp4"
                recording_url = upload_to_s3(output_path, s3_key)
                rec["recording_url"] = recording_url
                rec["status"] = "complete"
                rec["completed_at"] = now.isoformat()
                logger.info(f"[Recorder:{session_id}] Recording complete: {recording_url}")

                # Notify relay
                _notify_relay(session_id, amplinar_id, recording_url)

                # Clean up temp file
                try:
                    os.remove(output_path)
                except Exception:
                    pass
            except Exception as e:
                logger.error(f"[Recorder:{session_id}] S3 upload failed: {e}")
                rec["status"] = "upload_failed"
                rec["error"] = str(e)
        else:
            if rec.get("status") == "recording":
                rec["status"] = "error"
                rec["error"] = "Output file missing or empty"


def _notify_relay(session_id: str, amplinar_id: str, recording_url: str) -> None:
    """POST recording_complete notification to the relay server."""
    if not RELAY_URL:
        logger.warning("[Recorder] RELAY_URL not set — skipping relay notification")
        return
    try:
        resp = requests.post(
            f"{RELAY_URL}/api/session/recording-complete",
            json={
                "session_id": session_id,
                "amplinar_id": amplinar_id,
                "s3_url": recording_url,
                "recorded_at": datetime.now(timezone.utc).isoformat(),
            },
            headers={"x-api-key": RELAY_API_KEY},
            timeout=10,
        )
        logger.info(f"[Recorder] Relay notified: {resp.status_code}")
    except Exception as e:
        logger.error(f"[Recorder] Relay notification failed: {e}")


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "amplinar-recorder"})


@app.route("/start", methods=["POST"])
def start_recording():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    viewer_url = data.get("viewer_url")
    session_id = data.get("session_id")
    amplinar_id = data.get("amplinar_id")

    if not viewer_url or not session_id or not amplinar_id:
        return jsonify({"error": "viewer_url, session_id, and amplinar_id are required"}), 400

    with _recording_lock:
        global _recording
        if _recording and _recording.get("status") == "recording":
            return jsonify({"error": "A recording is already in progress", "session_id": _recording["session_id"]}), 409

        rec = {
            "session_id": session_id,
            "amplinar_id": amplinar_id,
            "viewer_url": viewer_url,
            "status": "starting",
            "stop_event": threading.Event(),
            "started_at": None,
            "completed_at": None,
            "recording_url": None,
            "error": None,
        }
        _recording = rec

    thread = threading.Thread(target=_recording_worker, args=(rec,), daemon=True)
    thread.start()

    logger.info(f"[Recorder] Started recording for session {session_id}, amplinar {amplinar_id}")
    return jsonify({"status": "started", "session_id": session_id})


@app.route("/stop", methods=["POST"])
def stop_recording():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data = request.get_json() or {}
    session_id = data.get("session_id")

    with _recording_lock:
        rec = _recording

    if not rec:
        return jsonify({"error": "No active recording"}), 404

    if session_id and rec["session_id"] != session_id:
        return jsonify({"error": f"Active recording is for session {rec['session_id']}, not {session_id}"}), 409

    if rec["status"] not in ("recording", "starting"):
        return jsonify({"status": rec["status"], "session_id": rec["session_id"]})

    logger.info(f"[Recorder] Stopping recording for session {rec['session_id']}")
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
        "status": rec["status"],
        "session_id": rec["session_id"],
        "amplinar_id": rec["amplinar_id"],
        "started_at": rec.get("started_at"),
        "completed_at": rec.get("completed_at"),
        "recording_url": rec.get("recording_url"),
        "error": rec.get("error"),
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
