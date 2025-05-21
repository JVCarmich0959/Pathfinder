#!/usr/bin/env bash
# setup_dev_env.sh - Initialize local development environment
#
# 1. Ensure .env exists
# 2. Install Python dependencies
# 3. Start docker compose services
set -euo pipefail

# Step 1: environment file
if [ ! -f .env ]; then
    cp .env.example .env
    echo "Created .env from template. Please edit ACLED_TOKEN and ACLED_EMAIL." >&2
fi

# Step 2: Python deps (optional if using containers only)
if command -v pip >/dev/null 2>&1; then
    pip install -r requirements.txt
fi

# Step 3: start Docker services
echo "Starting docker containers..."
docker compose up -d

echo "\n✅  Development environment is starting." \
     "Access Jupyter at http://localhost:8888 when ready." >&2
