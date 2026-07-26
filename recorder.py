"""
Amplinar Recorder Service
=========================
Records a live Amplinar session using overlapping Browserless chunks stitched
together with FFmpeg — no audible gaps.

Each chunk is recorded by a Node.js subprocess (record_chunk.js) using
Puppeteer-core. When /stop is called, the current chunk subprocess receives
SIGTERM, which triggers record_chunk.js to stop recording early and save
whatever was captured.

Strategy
--------
- CHUNK_DURATION seconds per chunk (default 810s = 13.5 min, under 15-min limit)
- OVERLAP seconds before a chunk ends, the next chunk starts (default 20s)
- First OVERLAP seconds of each chunk after the first are trimmed by record_chunk.js
- FFmpeg concat demuxer joins all chunks seamlessly
- On /stop: SIGTERM sent to current chunk subprocess → it saves early → concat + upload

API
---
  POST /start   { "viewer_url": "...", "session_id": "...", "amplinar_id": "..." }
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
from datetime import datetime, timezone
from typing import Optional

import boto3
import requests
from flask import Flask, jsonify, request

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
CHUNK_DURATION       = int(os.environ.get("CHUNK_DURATION", "810"))   # seconds
OVERLAP              = int(os.environ.get("OVERLAP", "20"))            # seconds

CHUNK_JS = os.path.join(os.path.dirname(__file__), "record_chunk.js")

# ── State ─────────────────────────────────────────────────────────────────────
_recording: Optional[dict] = None
_recording_lock = threading.Lock()


# ── Auth ──────────────────────────────────────────────────────────────────────
def check_auth() -> bool:
    if not RECORDER_API_KEY:
        return True
    return request.headers.get("X-Recorder-Key") == RECORDER_API_KEY


# ── Record one chunk via Node.js subprocess ───────────────────────────────────
def _record_chunk_subprocess(
    viewer_url: str,
    duration_secs: int,
    trim_secs: int,
    output_path: str,
    proc_holder: dict,          # {"proc": None} — filled in so caller can SIGTERM
) -> None:
    """
    Launches record_chunk.js as a Popen subprocess and waits for it.
    Stores the Popen object in proc_holder["proc"] so the stop handler
    can send SIGTERM to trigger early-stop recording.
    Raises RuntimeError on failure.
    """
    env = os.environ.copy()
    env["BROWSERLESS_API_KEY"] = BROWSERLESS_API_KEY

    proc = subprocess.Popen(
        [
            "node",
            CHUNK_JS,
            viewer_url,
            str(duration_secs * 1000),   # ms
            str(trim_secs * 1000),        # ms
            output_path,
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    proc_holder["proc"] = proc

    stdout, stderr = proc.communicate(timeout=duration_secs + 120)

    if stdout:
        logger.info(f"[chunk stdout] {stdout.strip()}")
    if proc.returncode != 0:
        if stderr:
            logger.error(f"[chunk stderr] {stderr.strip()}")
        raise RuntimeError(
            f"record_chunk.js exited {proc.returncode}: {stderr.strip()[-300:]}"
        )


# ── FFmpeg concat ─────────────────────────────────────────────────────────────
def _concat_chunks(chunk_paths: list) -> bytes:
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
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", list_path, "-c", "copy", out_path],
            check=True,
            capture_output=True,
        )
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        os.unlink(list_path)
        os.unlink(out_path)


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


# ── Main recording worker ─────────────────────────────────────────────────────
def _recording_worker(rec: dict) -> None:
    """
    Orchestrates overlapping chunks.

    Stop mechanism:
      - stop_event is set by /stop
      - current_proc_holder["proc"] holds the active Popen object
      - When stop_event fires, we send SIGTERM to the subprocess, which
        causes record_chunk.js to stop recording early and save the file
      - We then wait for the subprocess to exit, concat, and upload
    """
    session_id  = rec["session_id"]
    amplinar_id = rec["amplinar_id"]
    viewer_url  = rec["viewer_url"]
    stop_event  = rec["stop_event"]

    if not BROWSERLESS_API_KEY:
        rec["status"] = "error"
        rec["error"]  = "BROWSERLESS_API_KEY not configured"
        return

    chunk_files: list = []
    chunk_index = 0
    current_proc_holder: dict = {"proc": None}
    rec["_proc_holder"] = current_proc_holder   # expose so stop handler can SIGTERM

    rec["started_at"] = datetime.now(timezone.utc).isoformat()
    rec["status"]     = "recording"
    logger.info(f"[Recorder:{session_id}] Starting (chunk={CHUNK_DURATION}s, overlap={OVERLAP}s)")

    try:
        while True:
            trim = 0 if chunk_index == 0 else OVERLAP

            # Create temp file for this chunk
            tf = tempfile.NamedTemporaryFile(suffix=f"_chunk{chunk_index}.webm", delete=False)
            tf.close()
            chunk_path = tf.name
            chunk_files.append(chunk_path)

            chunk_result: dict = {}
            chunk_done = threading.Event()

            def _do_chunk(idx, path, trim_s, result, done, holder):
                try:
                    logger.info(f"[Recorder:{session_id}] Chunk {idx} start (trim={trim_s}s)")
                    _record_chunk_subprocess(viewer_url, CHUNK_DURATION, trim_s, path, holder)
                    result["ok"] = True
                except Exception as ex:
                    result["ok"]    = False
                    result["error"] = str(ex)
                finally:
                    done.set()

            t = threading.Thread(
                target=_do_chunk,
                args=(chunk_index, chunk_path, trim, chunk_result, chunk_done, current_proc_holder),
                daemon=True,
            )
            t.start()

            # Wait until OVERLAP seconds before this chunk ends, then start next
            overlap_wait = CHUNK_DURATION - OVERLAP
            stop_signalled = stop_event.wait(timeout=overlap_wait)

            if stop_signalled:
                # Send SIGTERM to the Node.js subprocess so it stops recording
                # early and saves whatever it has
                proc = current_proc_holder.get("proc")
                if proc and proc.poll() is None:
                    logger.info(f"[Recorder:{session_id}] Sending SIGTERM to chunk {chunk_index}")
                    try:
                        proc.send_signal(signal.SIGTERM)
                    except Exception as e:
                        logger.warning(f"[Recorder:{session_id}] SIGTERM failed: {e}")

                # Wait for the subprocess to finish (it will save the partial recording)
                chunk_done.wait(timeout=60)

                if not chunk_result.get("ok"):
                    logger.warning(
                        f"[Recorder:{session_id}] Chunk {chunk_index} returned error after stop: "
                        f"{chunk_result.get('error')} — checking if file exists anyway"
                    )
                    # Even if returncode != 0, the file may have been written
                    if not os.path.exists(chunk_path) or os.path.getsize(chunk_path) == 0:
                        raise RuntimeError(
                            f"Chunk {chunk_index} failed and no output file: {chunk_result.get('error')}"
                        )
                    logger.info(f"[Recorder:{session_id}] Chunk {chunk_index} file exists despite error — using it")

                sz = os.path.getsize(chunk_path) if os.path.exists(chunk_path) else 0
                logger.info(f"[Recorder:{session_id}] Chunk {chunk_index} done ({sz} bytes)")
                break

            else:
                # Overlap window: start next chunk, wait for this one to finish
                chunk_index += 1
                chunk_done.wait()

                if not chunk_result.get("ok"):
                    raise RuntimeError(f"Chunk {chunk_index - 1} failed: {chunk_result.get('error')}")

                sz = os.path.getsize(chunk_path)
                logger.info(f"[Recorder:{session_id}] Chunk {chunk_index - 1} done ({sz} bytes)")
                # Loop continues to start next chunk

        # ── Post-recording: concat + upload ───────────────────────────────────
        # Filter out any zero-byte files
        valid_chunks = [f for f in chunk_files if os.path.exists(f) and os.path.getsize(f) > 0]
        if not valid_chunks:
            raise RuntimeError("No valid chunk files to upload")

        rec["status"] = "concatenating"
        logger.info(f"[Recorder:{session_id}] Concatenating {len(valid_chunks)} chunk(s)")
        final_bytes = _concat_chunks(valid_chunks)

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
    finally:
        for f in chunk_files:
            try:
                os.unlink(f)
            except Exception:
                pass


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify({"status": "ok", "service": "amplinar-recorder", "mode": "puppeteer-chunked"})


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
            "_proc_holder":  {"proc": None},
            "started_at":    None,
            "completed_at":  None,
            "recording_url": None,
            "error":         None,
        }
        _recording = rec

    threading.Thread(target=_recording_worker, args=(rec,), daemon=True).start()
    logger.info(f"[Recorder] Started session={session_id} amplinar={amplinar_id}")
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

    # Also SIGTERM the subprocess immediately (belt-and-suspenders)
    proc = rec.get("_proc_holder", {}).get("proc")
    if proc and proc.poll() is None:
        logger.info(f"[Recorder] Sending SIGTERM to subprocess from /stop route")
        try:
            proc.send_signal(signal.SIGTERM)
        except Exception as e:
            logger.warning(f"[Recorder] SIGTERM from route failed: {e}")

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
