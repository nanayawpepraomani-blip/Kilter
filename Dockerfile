# syntax=docker/dockerfile:1.7
#
# Kilter — production container image.
#
# Multi-stage build:
#   1. builder:  installs pinned wheels into /install (needs build-essential
#                + libffi-dev for cryptography).
#   2. runtime:  thin python:3.12-slim with only runtime libs, the wheels
#                copied across, app code, non-root user.
#
# Persistent state (kilter.db, messages/, uploads/, exports/) is expected to
# live on volumes mounted in via docker-compose. The encryption key is
# expected to come in via the KILTER_SECRET_KEY env var — never bake it
# into the image.

# ---------- stage 1: builder ----------
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt

# ---------- stage 2: runtime ----------
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    KILTER_DB_PATH=/data/kilter.db

# Runtime libs only. curl is included for the HEALTHCHECK; libffi for
# cryptography's runtime; tini gives us a clean PID 1 (signal handling +
# zombie reaping) without pulling in an entire init system.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libffi8 \
        curl \
        tini \
    && rm -rf /var/lib/apt/lists/*

# Non-root account. UID/GID 10001 to stay clear of host system users.
RUN groupadd --system --gid 10001 kilter \
 && useradd  --system --uid 10001 --gid kilter --home-dir /app --shell /sbin/nologin kilter

COPY --from=builder /install /usr/local

WORKDIR /app

# Persistent dirs created up-front so volume mounts inherit ownership.
RUN mkdir -p /data /app/messages /app/uploads /app/exports \
 && chown -R kilter:kilter /data /app

# App code last so unrelated edits don't bust the deps layer.
COPY --chown=kilter:kilter . /app

USER kilter

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/healthz || exit 1

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--proxy-headers", "--forwarded-allow-ips=*"]
