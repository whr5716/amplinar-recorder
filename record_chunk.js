/**
 * record_chunk.js
 * ---------------
 * Records a single chunk of a Browserless session using Puppeteer.
 * Called by recorder.py as a subprocess.
 *
 * Usage:
 *   node record_chunk.js <viewer_url> <duration_ms> <trim_ms> <output_path>
 *
 * Exits 0 on success, non-zero on failure.
 * Writes trimmed WebM bytes to <output_path>.
 *
 * Join gate: after navigating to viewer.html, the recorder auto-fills
 * the name and email fields and clicks "Join Session" so the Browserless
 * browser actually enters the session before recording starts.
 *
 * Stop behaviour: on SIGTERM, stops recording early and saves whatever
 * was captured — so /stop can kill the subprocess and still get a file.
 */

const puppeteer    = require("puppeteer-core");
const fs           = require("fs");
const { execSync, spawnSync } = require("child_process");

const [,, viewerUrl, durationMs, trimMs, outputPath] = process.argv;

if (!viewerUrl || !durationMs || !outputPath) {
  console.error("Usage: node record_chunk.js <viewer_url> <duration_ms> <trim_ms> <output_path>");
  process.exit(1);
}

const TOKEN    = process.env.BROWSERLESS_API_KEY;
const DURATION = parseInt(durationMs, 10);
const TRIM     = parseInt(trimMs, 10) || 0;

// Recorder identity — shown in the host panel viewer list
const RECORDER_NAME  = process.env.RECORDER_NAME  || "Amplinar Recorder";
const RECORDER_EMAIL = process.env.RECORDER_EMAIL || "recorder@amplinar.com";

if (!TOKEN) {
  console.error("BROWSERLESS_API_KEY not set");
  process.exit(1);
}

const WS_ENDPOINT = `wss://production-sfo.browserless.io?token=${TOKEN}&headless=false&stealth&record=true&timeout=900000`;

// ── Graceful stop ─────────────────────────────────────────────────────────────
// When recorder.py kills us (SIGTERM), we want to stop recording and save
// whatever we have rather than dying with no output.
let _stopRecording = null;   // set to a function once recording starts
let _stopping = false;

process.on("SIGTERM", () => {
  console.log("[chunk] SIGTERM received — stopping recording early");
  _stopping = true;
  if (_stopRecording) {
    _stopRecording();
  }
});

// ── Helpers ───────────────────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  let browser;
  try {
    console.log(`[chunk] Connecting to Browserless (duration=${DURATION}ms, trim=${TRIM}ms)`);
    browser = await puppeteer.connect({ browserWSEndpoint: WS_ENDPOINT });

    const pages = await browser.pages();
    const page  = pages.length > 0 ? pages[0] : await browser.newPage();

    await page.setViewport({ width: 1280, height: 720 });

    // Capture browser console logs so we can diagnose LiveKit/video issues
    page.on('console', msg => {
      const type = msg.type();
      const text = msg.text();
      if (type === 'error' || text.includes('[LK]') || text.includes('[chunk]') || text.includes('Video') || text.includes('audio') || text.includes('play()') || text.includes('NotAllowed') || text.includes('readyState')) {
        console.log(`[page:${type}] ${text}`);
      }
    });

    await page.goto(viewerUrl, { waitUntil: "domcontentloaded", timeout: 30000 });

    // ── Auto-join the session gate ────────────────────────────────────────────
    // Wait up to 10s for the name input to appear (gate may take a moment to render)
    console.log("[chunk] Waiting for join gate...");
    try {
      await page.waitForSelector("#name-input", { visible: true, timeout: 10000 });

      // Fill in name
      await page.click("#name-input", { clickCount: 3 });
      await page.type("#name-input", RECORDER_NAME);

      // Fill in email if the field exists
      const emailEl = await page.$("#email-input");
      if (emailEl) {
        await page.click("#email-input", { clickCount: 3 });
        await page.type("#email-input", RECORDER_EMAIL);
      }

      // Small pause so validation can run
      await sleep(500);

      // Click Join Session button
      const joinBtn = await page.$("#join-btn");
      if (joinBtn) {
        await joinBtn.click();
        console.log("[chunk] Clicked Join Session");
      } else {
        console.log("[chunk] No join button found — proceeding anyway");
      }

      // Wait briefly for the gate to dismiss and the session to load
      await sleep(3000);
      console.log("[chunk] Join gate passed");
    } catch (gateErr) {
      // Gate may not be present (e.g. session already live and gate dismissed)
      console.log("[chunk] Join gate not found or already dismissed:", gateErr.message);
    }

    // ── Background gate watcher ───────────────────────────────────────────────
    // The viewer page shows a "Tap to Rejoin" or "Tap to Begin" overlay whenever
    // it needs a user gesture to unlock AudioContext/autoplay. Poll for these
    // overlays every 3 seconds and click through them automatically.
    const gateWatcher = setInterval(async () => {
      if (_stopping) { clearInterval(gateWatcher); return; }
      try {
        const clicked = await page.evaluate(() => {
          const gates = [
            { id: 'session-tap-gate', btn: 'button[onclick="sessionTapUnlock()"]' },
            { id: 'ios-start-gate',   btn: 'button[onclick="iosStartTap()"]' },
          ];
          for (const g of gates) {
            const el = document.getElementById(g.id);
            if (el && (el.style.display === 'flex' || el.style.display === 'block')) {
              const btn = document.querySelector(g.btn);
              if (btn) { btn.click(); return g.id; }
            }
          }
          return null;
        });
        if (clicked) console.log('[chunk] Clicked through gate:', clicked);
      } catch (_) { /* page may be navigating */ }
    }, 3000);

    // ── Start recording ───────────────────────────────────────────────────────
    const cdp = await page.createCDPSession();
    await cdp.send("Browserless.startRecording");
    console.log("[chunk] Recording started");

    // Wait for duration OR early stop signal
    await new Promise(resolve => {
      const timer = setTimeout(resolve, DURATION);
      _stopRecording = () => {
        clearTimeout(timer);
        resolve();
      };
    });

    clearInterval(gateWatcher);
    console.log("[chunk] Stopping recording");
    const response = await cdp.send("Browserless.stopRecording", { encoding: "base64" });
    const rawBytes = Buffer.from(response.value, "base64");
    console.log(`[chunk] Got ${rawBytes.length} bytes`);

    if (rawBytes.length === 0) {
      throw new Error("Empty recording returned from Browserless");
    }

    if (TRIM > 0) {
      // Write raw to temp, trim with FFmpeg, write trimmed to output
      const tmpPath = outputPath + ".raw.webm";
      fs.writeFileSync(tmpPath, rawBytes);
      const trimSecs = (TRIM / 1000).toFixed(3);
      const r = spawnSync("ffmpeg", ["-y", "-ss", trimSecs, "-i", tmpPath, "-c", "copy", outputPath], { stdio: "pipe" });
      fs.unlinkSync(tmpPath);
      if (r.status !== 0) {
        throw new Error(`FFmpeg trim failed: ${r.stderr.toString().slice(-200)}`);
      }
      console.log(`[chunk] Trimmed ${trimSecs}s, saved to ${outputPath}`);
    } else {
      fs.writeFileSync(outputPath, rawBytes);
      console.log(`[chunk] Saved to ${outputPath}`);
    }

    await browser.close();
    process.exit(0);
  } catch (err) {
    console.error("[chunk] Error:", err.message);
    if (browser) {
      try { await browser.close(); } catch (_) {}
    }
    process.exit(1);
  }
})();
