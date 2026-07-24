# amplinar-recorder

Dedicated Railway microservice that records each live Amplinar session as a single continuous MP4 by running a headless Chromium browser pointed at `viewer.html`, capturing the virtual display with FFmpeg, and uploading the result to S3.

## Architecture

```
amplinar-relay  ──POST /start──►  amplinar-recorder
                                       │
                                  Xvfb + Chromium
                                  (loads viewer.html)
                                       │
                                  FFmpeg x11grab
                                       │
               ◄──POST /stop───  (last video_ended)
                                       │
                                  Upload MP4 to S3
                                       │
               ◄──POST /api/session/recording-complete
```

## API

| Method | Path      | Description                                      |
|--------|-----------|--------------------------------------------------|
| GET    | /health   | Health check                                     |
| POST   | /start    | Start recording a session                        |
| POST   | /stop     | Stop recording (triggers upload + relay notify)  |
| GET    | /status   | Get current recording status                     |

### POST /start

```json
{
  "viewer_url": "https://amplinar-relay-production.up.railway.app/viewer.html?id=abc123",
  "session_id": "session-uuid",
  "amplinar_id": "amplinar-uuid"
}
```

### POST /stop

```json
{
  "session_id": "session-uuid"
}
```

## Environment Variables

| Variable              | Description                                    |
|-----------------------|------------------------------------------------|
| `RECORDER_API_KEY`    | Shared secret (X-Recorder-Key header)          |
| `S3_ACCESS_KEY_ID`    | AWS access key                                 |
| `S3_SECRET_ACCESS_KEY`| AWS secret key                                 |
| `S3_BUCKET_NAME`      | S3 bucket (wholesalehotelrates-images)         |
| `S3_REGION`           | AWS region (us-east-1)                         |
| `RELAY_URL`           | Relay base URL for recording_complete callback |
| `RELAY_API_KEY`       | Relay API key for the callback                 |
| `PORT`                | HTTP port (default: 8080)                      |

## S3 Key Format

```
amplinar-recordings/{amplinarId}/{YYYYMMDD_HHMMSS}_{sessionId}.mp4
```

## Railway Deployment

1. Create a new Railway service pointing to this repo
2. Set all environment variables listed above
3. Railway will auto-detect the Dockerfile and build/deploy
