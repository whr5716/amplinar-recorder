FROM python:3.11-slim

# Install system dependencies: Xvfb, Chromium, FFmpeg, PulseAudio
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    chromium \
    ffmpeg \
    pulseaudio \
    pulseaudio-utils \
    dbus \
    dbus-x11 \
    libglib2.0-0 \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    fonts-liberation \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# PulseAudio config for virtual audio capture
RUN mkdir -p /root/.config/pulse && \
    echo "default-server = unix:/tmp/pulse-socket" > /root/.config/pulse/client.conf

EXPOSE 8080

CMD ["python", "recorder.py"]
