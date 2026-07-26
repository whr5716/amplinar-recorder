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

# PulseAudio config for headless Docker container
# 1. Add root to pulse-access group
RUN adduser root pulse-access || true

# 2. Disable module-console-kit (requires ConsoleKit/D-Bus, not available in containers)
RUN sed -i 's/^load-module module-console-kit/# load-module module-console-kit/' /etc/pulse/default.pa || true

# 3. Set up D-Bus machine ID (required by PulseAudio even in --system mode)
RUN mkdir -p /var/run/dbus && \
    dbus-uuidgen > /var/lib/dbus/machine-id 2>/dev/null || true

# 4. PulseAudio client config — point to system mode socket
# Do NOT set autospawn=no or daemon-binary=/bin/true — those break pactl
RUN mkdir -p /root/.config/pulse && \
    echo 'default-server = unix:/var/run/pulse/native' > /root/.config/pulse/client.conf

# 5. Create entrypoint that starts D-Bus then the app
RUN echo '#!/bin/bash\n\
mkdir -p /var/run/dbus\n\
dbus-daemon --system --fork 2>/dev/null || true\n\
exec gunicorn --bind 0.0.0.0:8080 --workers 1 --threads 4 --timeout 120 recorder:app' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

EXPOSE 8080

CMD ["/entrypoint.sh"]
