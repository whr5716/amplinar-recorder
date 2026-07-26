"""
Amplinar Recorder Service
=========================
Records a live Amplinar session using overlapping Browserless chunks stitched
together with FFmpeg — no audible gaps.

Strategy
--------
- Each chunk runs for CHUNK_DURATION seconds (default 810s = 13.5 min, well
  under the 15-min Browserless plan limit).
- OVERLAP seconds (default 20) before a chunk ends, the next chunk starts so
  both are recording simultaneously.
- When concatenating, the first OVERLAP seconds of every chunk after the first
  are trimmed, giving a seamless join.

API
---
  POST /start   { "viewer_url": "...", "session_id": "...", "amplinar_id": "..." }
  POST /stop    { "session_id": "..." }
  GET  /status
  GET  /health

Environment variables
---------------------
  BROWSERLESS_API_KEY    Browserless token
  RECORDER_API_KEY       Shared secret — relay must send X-Recorder-Key header
  S3_ACCESS_KEY_ID / S3_SECRET_ACCESS_KEY / S3_BUCKET_NAME / S3_REGION
  RELAY_URL / RELAY_API_KEY
  CHUNK_DURATION         Seconds per chunk (default 810)
  OVERLAP                Overlap seconds between chunks (default 20)
"""
from __future__ import annotations

import base64
import logging
import os
import subprocess
import tempfile
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
BROWSERLESS_API_KEY  = os.environ.get("BROWSERLESS_API_KEY", "")
RECORDER_API_KEY     = os.environ.get("RECORDER_API_KEY", "")
S3_ACCESS_KEY_ID     = os.environ.get("S3_ACCESS_KEY_ID", "")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY", "")
S3_BUCKET_NAME       = os.environ.get("S3_BUCKET_NAME", "wholesalehotelrates-images")
S3_REGION            = os.environ.get("S3_REGION", "us-east-1")
RELAY_URL            = os.environ.get("RELAY_URL", "")
RELAY_API_KEY        = os.environ.get("RELAY_API_KEY", "")
CHUNK_DURATION       = int(os.environ.get("CHUNK_DURATION", "810"))   # 13.5 min
OVERLAP              = int(os.environ.get("OVERLAP", "20"))            # 20 sec overlap

# ── State ─────────────────────────────────────────────────────────────────────
_recording: Optional[dict] = None
_recording_lock = threading.Lock()


# ── Auth ──────────────────────────────────────────────────────────────────────
def check_auth() -> bool:
    if not RECORDER_API_KEY:
        return True
    return request.headers.get("X-Recorder-Key") == RECORDER_API_KEY


# ── Browserless chunk recorder ────────────────────────────────────────────────
def _record_chunk(viewer_url: str, duration_secs: int, trim_start: float = 0.0) -> bytes:
    """
    Connect to Browserless, navigate to viewer_url, record for duration_secs,
    return the raw WebM bytes (already trimmed if trim_start > 0).
    """
    ws_endpoint = (
        f"wss://production-sfo.browserless.io"
        f"?token={BROWSERLESS_API_KEY}"
        f"&headless=false"
        f"&stealth"
        f"&record=true"
        f"&timeout=900000"
    )

    with sync_playwright() as pw:
        browser = pw.chromium.connect_over_cdp(ws_endpoint)
        context = browser.contexts[0]
        page    = context.pages[0]

        page.set_viewport_size({"width": 1280, "height": 720})
        page.goto(viewer_url, wait_until="domcontentloaded", timeout=30000)

        cdp = context.new_cdp_session(page)
        cdp.send("Browserless.startRecording")

        # Wait for the chunk duration
        page.wait_for_timeout(duration_secs * 1000)

        response  = cdp.send("Browserless.stopRecording", {"encoding": "base64"})
        raw_bytes = base64.b64decode(response["value"])
        browser.close()

    if not raw_bytes:
        raise RuntimeError("Browserless returned empty recording")

    if trim_start <= 0:
        return raw_bytes

    # Trim the first trim_start seconds using FFmpeg
    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as inf:
        inf.write(raw_bytes)
        in_path = inf.name

    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as outf:
        out_path = outf.name

    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-ss", str(trim_start),
                "-i", in_path,
                "-c", "copy",
                out_path,
            ],
            check=True,
            capture_output=True,
        )
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        os.unlink(in_path)
        os.unlink(out_path)


# ── FFmpeg concat ─────────────────────────────────────────────────────────────
def _concat_chunks(chunk_paths: list[str]) -> bytes:
    """Concatenate WebM chunk files into a single WebM using FFmpeg concat demuxer."""
    if len(chunk_paths) == 1:
        with open(chunk_paths[0], "rb") as f:
            return f.read()

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as listf:
        for p in chunk_paths:
            listf.write(f"file '{p}'\n")
        list_path = listf.name

    with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as outf:
        out_path = outf.name

    try:
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-f", "concat",
                "-safe", "0",
                "-i", list_path,
                "-c", "copy",
                out_path,
            ],
            check=True,
            capture_output=True,
        )
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        os.unlink(list_path)
        os.unlink(out_path)


# ── S3 upload ─────────────────────────────────────────────────────────────────
def upload_to_s3(data: bytes, s3_key: str, content_type: str = "video/webm") -> str:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
    )
    logger.info(f"[S3] Uploading {len(data)} bytes → s3://{S3_BUCKET_NAME}/{s3_key}")
    s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=data, ContentType=content_type)
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


# ── Main recording worker ─────────────────────────────────────────────────────
def _recording_worker(rec: dict) -> None:
    """
    Orchestrates overlapping chunks:
      chunk 0: records for CHUNK_DURATION seconds (no trim)
      chunk N: starts OVERLAP seconds before chunk N-1 ends, trimmed by OVERLAP
    Chunks are concatenated and uploaded when stop is signalled.
    """
    session_id  = rec["session_id"]
    amplinar_id = rec["amplinar_id"]
    viewer_url  = rec["viewer_url"]
    stop_event  = rec["stop_event"]

    if not BROWSERLESS_API_KEY:
        rec["status"] = "error"
        rec["error"]  = "BROWSERLESS_API_KEY not configured"
        return

    chunk_files: list[str] = []
    chunk_index = 0

    rec["started_at"] = datetime.now(timezone.utc).isoformat()
    rec["status"]     = "recording"
    logger.info(f"[Recorder:{session_id}] Starting chunked recording (chunk={CHUNK_DURATION}s, overlap={OVERLAP}s)")

    try:
        while not stop_event.is_set():
            is_first = (chunk_index == 0)
            trim      = 0.0 if is_first else float(OVERLAP)

            # For chunks after the first, this thread was spawned OVERLAP seconds
            # before the previous chunk ended — so we start immediately.
            # Duration: CHUNK_DURATION for all chunks; stop_event checked after.
            logger.info(f"[Recorder:{session_id}] Recording chunk {chunk_index} (trim={trim}s)")

            # Use a thread so we can start the next chunk while this one finishes
            chunk_result: dict = {}
            chunk_event  = threading.Event()

            def _do_chunk(duration: int, trim_s: float, result: dict, done: threading.Event):
                try:
                    data = _record_chunk(viewer_url, duration, trim_s)
                    result["data"]  = data
                    result["error"] = None
                except Exception as ex:
                    result["data"]  = None
                    result["error"] = str(ex)
                finally:
                    done.set()

            chunk_thread = threading.Thread(
                target=_do_chunk,
                args=(CHUNK_DURATION, trim, chunk_result, chunk_event),
                daemon=True,
            )
            chunk_thread.start()

            # Wait until OVERLAP seconds before this chunk ends, then start next
            # chunk (unless stop was requested).
            overlap_wait = CHUNK_DURATION - OVERLAP
            stop_signalled = stop_event.wait(timeout=overlap_wait)

            if not stop_signalled:
                # Start next chunk immediately (overlap window begins)
                chunk_index += 1
                # Loop continues — next iteration starts the new chunk while
                # the current chunk_thread is still recording its final OVERLAP seconds.
                # We need to wait for the current chunk to finish before looping.
                chunk_event.wait()
            else:
                # Stop requested — wait for current chunk to finish
                chunk_event.wait()

            if chunk_result.get("error"):
                raise RuntimeError(f"Chunk {chunk_index} failed: {chunk_result['error']}")

            # Save chunk to temp file
            with tempfile.NamedTemporaryFile(suffix=f"_chunk{chunk_index}.webm", delete=False) as tf:
                tf.write(chunk_result["data"])
                chunk_files.append(tf.name)
                logger.info(f"[Recorder:{session_id}] Chunk {chunk_index} saved ({len(chunk_result['data'])} bytes)")

            if stop_signalled:
                break

        # Concatenate all chunks
        rec["status"] = "concatenating"
        logger.info(f"[Recorder:{session_id}] Concatenating {len(chunk_files)} chunk(s)")
        final_bytes = _concat_chunks(chunk_files)
        logger.info(f"[Recorder:{session_id}] Final video: {len(final_bytes)} bytes")

        # Upload to S3
        rec["status"] = "uploading"
        now    = datetime.now(timezone.utc)
        s3_key = f"amplinar-recordings/{amplinar_id}/{now.strftime('%Y%m%d_%H%M%S')}_{session_id}.webm"
        recording_url = upload_to_s3(final_bytes, s3_key)

        rec["recording_url"] = recording_url
        rec["status"]        = "complete"
        rec["completed_at"]  = now.isoformat()
        logger.info(f"[Recorder:{session_id}] Complete: {recording_url}")

        _notify_relay(session_id, amplinar_id, recording_url)

    except Exception as e:
        logger.error(f"[Recorder:{session_id}] Failed: {e}")
        rec["status"] = "error"
        rec["error"]  = str(e)
    finally:
        for f in chunk_files:
            try:
                os.unlink(f)
            except Exception:
                pass


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "amplinar-recorder", "mode": "browserless-chunked"})


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
        if _recording and _recording.get("status") in ("recording", "starting"):
            return jsonify({
                "error": "A recording is already in progress",
                "session_id": _recording["session_id"],
            }), 409

        rec = {
            "session_id":    session_id,
            "amplinar_id":   amplinar_id,
            "viewer_url":    viewer_url,
            "status":        "starting",
            "stop_event":    threading.Event(),
            "started_at":    None,
            "completed_at":  None,
            "recording_url": None,
            "error":         None,
        }
        _recording = rec

    threading.Thread(target=_recording_worker, args=(rec,), daemon=True).start()
    logger.info(f"[Recorder] Started for session {session_id}, amplinar {amplinar_id}")
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

    logger.info(f"[Recorder] Stopping session {rec['session_id']}")
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
