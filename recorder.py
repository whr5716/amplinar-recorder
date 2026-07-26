"""
Amplinar Recorder Service
=========================
Records a live Amplinar session using the Browserless screen recording API
(Playwright + CDP), which captures both video and audio natively.
The resulting WebM is uploaded to S3 and the relay is notified.

API:
  POST /start   { "viewer_url": "...", "session_id": "...", "amplinar_id": "..." }
  POST /stop    { "session_id": "..." }
  GET  /status  ?session_id=...
  GET  /health

Environment variables:
  BROWSERLESS_API_KEY    Browserless token (from production-sfo.browserless.io)
  RECORDER_API_KEY       Shared secret — relay must send this in X-Recorder-Key header
  S3_ACCESS_KEY_ID       AWS access key
  S3_SECRET_ACCESS_KEY   AWS secret key
  S3_BUCKET_NAME         S3 bucket name (wholesalehotelrates-images)
  S3_REGION              AWS region (default: us-east-1)
  RELAY_URL              Relay base URL for posting recording_complete notification
  RELAY_API_KEY          Relay API key for the notification callback
"""
from __future__ import annotations

import base64
import logging
import os
import threading
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
from flask import Flask, jsonify, request
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("amplinar-recorder")

app = Flask(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BROWSERLESS_API_KEY = os.environ.get("BROWSERLESS_API_KEY", "")
RECORDER_API_KEY    = os.environ.get("RECORDER_API_KEY", "")
S3_ACCESS_KEY_ID    = os.environ.get("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME      = os.environ.get("S3_BUCKET_NAME", "wholesalehotelrates-images")
S3_REGION           = os.environ.get("S3_REGION", "us-east-1")
RELAY_URL           = os.environ.get("RELAY_URL", "")
RELAY_API_KEY       = os.environ.get("RELAY_API_KEY", "")

# ── State ─────────────────────────────────────────────────────────────────────
_recording: Optional[dict] = None
_recording_lock = threading.Lock()


# ── Auth helper ───────────────────────────────────────────────────────────────
def check_auth() -> bool:
    if not RECORDER_API_KEY:
        return True
    return request.headers.get("X-Recorder-Key") == RECORDER_API_KEY


# ── S3 upload ─────────────────────────────────────────────────────────────────
def upload_to_s3(data: bytes, s3_key: str, content_type: str = "video/webm") -> str:
    """Upload bytes to S3 and return the public URL."""
    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
    )
    logger.info(f"[S3] Uploading {len(data)} bytes → s3://{S3_BUCKET_NAME}/{s3_key}")
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=data,
        ContentType=content_type,
    )
    url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
    logger.info(f"[S3] Upload complete: {url}")
    return url


# ── Relay notification ────────────────────────────────────────────────────────
def _notify_relay(session_id: str, amplinar_id: str, recording_url: str) -> None:
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


# ── Recording worker ──────────────────────────────────────────────────────────
def _recording_worker(rec: dict) -> None:
    """
    Runs in a background thread.
    Connects to Browserless with record=true, navigates to the viewer URL,
    starts recording via CDP, waits for stop signal, then uploads to S3.
    """
    session_id  = rec["session_id"]
    amplinar_id = rec["amplinar_id"]
    viewer_url  = rec["viewer_url"]

    if not BROWSERLESS_API_KEY:
        rec["status"] = "error"
        rec["error"]  = "BROWSERLESS_API_KEY not configured"
        logger.error(f"[Recorder:{session_id}] BROWSERLESS_API_KEY not set")
        return

    ws_endpoint = (
        f"wss://production-sfo.browserless.io"
        f"?token={BROWSERLESS_API_KEY}"
        f"&headless=false"
        f"&stealth"
        f"&record=true"
        f"&timeout=1800000"
    )

    logger.info(f"[Recorder:{session_id}] Connecting to Browserless (record=true)")

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.connect_over_cdp(ws_endpoint)
            context = browser.contexts[0]
            page    = context.pages[0]

            page.set_viewport_size({"width": 1280, "height": 720})
            logger.info(f"[Recorder:{session_id}] Navigating to {viewer_url}")
            page.goto(viewer_url, wait_until="domcontentloaded", timeout=30000)

            # Start recording via CDP
            cdp = context.new_cdp_session(page)
            cdp.send("Browserless.startRecording")
            rec["started_at"] = datetime.now(timezone.utc).isoformat()
            rec["status"]     = "recording"
            logger.info(f"[Recorder:{session_id}] Recording started")

            # Wait for stop signal
            rec["stop_event"].wait()
            logger.info(f"[Recorder:{session_id}] Stop signal received — finalizing")

            # Stop recording and retrieve WebM bytes
            response = cdp.send("Browserless.stopRecording", {"encoding": "base64"})
            webm_bytes = base64.b64decode(response["value"])
            logger.info(f"[Recorder:{session_id}] Got {len(webm_bytes)} bytes of WebM")

            browser.close()

        if len(webm_bytes) == 0:
            raise RuntimeError("Browserless returned empty recording")

        # Upload to S3
        rec["status"] = "uploading"
        now   = datetime.now(timezone.utc)
        s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_{session_id}.webm"
        recording_url = upload_to_s3(webm_bytes, s3_key, content_type="video/webm")
        rec["recording_url"] = recording_url
        rec["status"]        = "complete"
        rec["completed_at"]  = now.isoformat()
        logger.info(f"[Recorder:{session_id}] Complete: {recording_url}")

        _notify_relay(session_id, amplinar_id, recording_url)

    except Exception as e:
        logger.error(f"[Recorder:{session_id}] Recording failed: {e}")
        if rec.get("status") not in ("complete", "uploading"):
            rec["status"] = "error"
            rec["error"]  = str(e)


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "amplinar-recorder", "mode": "browserless"})


@app.route("/start", methods=["POST"])
def start_recording():
    if not check_auth():
        return jsonify({"error": "Unauthorized"}), 401

    data        = request.get_json() or {}
    viewer_url  = data.get("viewer_url")
    session_id  = data.get("session_id")
    amplinar_id = data.get("amplinar_id")

    if not viewer_url or not session_id or not amplinar_id:
        return jsonify({"error": "viewer_url, session_id, and amplinar_id are required"}), 400

    with _recording_lock:
        global _recording
        if _recording and _recording.get("status") == "recording":
            return jsonify({
                "error": "A recording is already in progress",
                "session_id": _recording["session_id"],
            }), 409

        rec = {
            "session_id":   session_id,
            "amplinar_id":  amplinar_id,
            "viewer_url":   viewer_url,
            "status":       "starting",
            "stop_event":   threading.Event(),
            "started_at":   None,
            "completed_at": None,
            "recording_url": None,
            "error":        None,
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
