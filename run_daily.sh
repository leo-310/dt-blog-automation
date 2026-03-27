#!/bin/zsh

set -euo pipefail

cd "/Users/cherubin/Desktop/blog agent"

if [[ -f ".env" ]]; then
  set -a
  source ".env"
  set +a
fi

source ".venv/bin/activate"
blog-agent generate
