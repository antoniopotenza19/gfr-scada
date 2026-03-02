#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi
echo "Running alembic upgrade head using DATABASE_URL=${DATABASE_URL:-<not-set>}"
alembic -c alembic.ini upgrade head
