FROM mcr.microsoft.com/devcontainers/go:1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --break-system-packages pre-commit
