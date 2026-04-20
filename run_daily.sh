#!/usr/bin/env sh

set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ -f ".env" ]; then
  set -a
  . ".env"
  set +a
fi

PYTHON_BIN=""
if [ -x ".venv/bin/python" ]; then
  PYTHON_BIN=".venv/bin/python"
elif [ -x ".venv/Scripts/python.exe" ]; then
  PYTHON_BIN=".venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "Python runtime not found. Set up .venv or install python/python3." >&2
  exit 1
fi

"$PYTHON_BIN" -c "import json; from blog_agent.api import BlogAgentApi; print(json.dumps(BlogAgentApi().automation_tick(force=False), ensure_ascii=False))"
