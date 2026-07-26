FROM python:3.11-slim

WORKDIR /app

# Install Node.js, FFmpeg, and system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ffmpeg \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Install puppeteer-core (no bundled browser — we connect to Browserless)
RUN npm install puppeteer-core

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "1", "--threads", "4", "--timeout", "7200", "recorder:app"]
