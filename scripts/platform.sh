#!/usr/bin/env bash
set -euo pipefail
project_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
python_exe="${project_root}/.venv/bin/python"
if [[ ! -x "$python_exe" ]]; then python_exe=python3; fi
export PYTHONPATH="${project_root}/src${PYTHONPATH:+:${PYTHONPATH}}"
exec "$python_exe" -m freewaypulse.platform "$@"
