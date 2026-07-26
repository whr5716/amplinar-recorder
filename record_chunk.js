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

// ── Main ──────────────────────────────────────────────────────────────────────
(async () => {
  let browser;
  try {
    console.log(`[chunk] Connecting to Browserless (duration=${DURATION}ms, trim=${TRIM}ms)`);
    browser = await puppeteer.connect({ browserWSEndpoint: WS_ENDPOINT });

    const pages = await browser.pages();
    const page  = pages.length > 0 ? pages[0] : await browser.newPage();

    await page.setViewport({ width: 1280, height: 720 });
    await page.goto(viewerUrl, { waitUntil: "domcontentloaded", timeout: 30000 });

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
