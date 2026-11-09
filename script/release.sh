#!/usr/bin/env bash
set -euo pipefail
export GIT_PAGER=cat GH_PAGER=cat PAGER=cat PYTHONDONTWRITEBYTECODE=1
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"
exec python3 "$SCRIPT_DIR/release.py" "$@"
