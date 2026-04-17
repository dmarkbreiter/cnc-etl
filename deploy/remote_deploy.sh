#!/usr/bin/env bash
set -euo pipefail

# Remote deploy helper for a single-Droplet systemd setup.
#
# Typical usage (from your laptop or GitHub Actions runner):
#   ssh deployer@<droplet> 'sudo /srv/cnc-etl/deploy/remote_deploy.sh'
#
# Assumptions:
# - Repo exists at /srv/cnc-etl and has an "origin" remote.
# - systemd units exist: cnc-prefect-server, cnc-prefect-worker
# - Prefect API is local: http://127.0.0.1:4200/api

APP_DIR="${APP_DIR:-/srv/cnc-etl}"
BRANCH="${BRANCH:-main}"
RUN_AS_USER="${RUN_AS_USER:-cnc}"
PREFECT_API_URL="${PREFECT_API_URL:-http://127.0.0.1:4200/api}"
PREFECT_HOME="${PREFECT_HOME:-/var/lib/prefect}"
GIT_PULL="${GIT_PULL:-1}"

SUDO="${SUDO:-sudo}"
PREFECT_API_WAIT_SECONDS="${PREFECT_API_WAIT_SECONDS:-60}"

systemctl_cmd() {
  if [[ "${EUID}" -eq 0 ]]; then
    systemctl "$@"
  else
    "${SUDO}" systemctl "$@"
  fi
}

run_as_user() {
  local cmd="$1"
  if [[ "${EUID}" -eq 0 ]]; then
    su -s /bin/bash - "${RUN_AS_USER}" -c "${cmd}"
  else
    "${SUDO}" -u "${RUN_AS_USER}" -H bash -lc "${cmd}"
  fi
}

wait_for_prefect_api() {
  local api_url="$1"
  local wait_seconds="$2"

  API_URL="${api_url}" WAIT_SECONDS="${wait_seconds}" python3 <<'PY'
import os
import socket
import sys
import time
from urllib.parse import urlparse

api_url = os.environ["API_URL"]
wait_seconds = int(os.environ["WAIT_SECONDS"])
parsed = urlparse(api_url)

host = parsed.hostname or "127.0.0.1"
port = parsed.port or (443 if parsed.scheme == "https" else 80)
deadline = time.time() + wait_seconds
last_error = None

while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=2):
            sys.exit(0)
    except OSError as exc:
        last_error = exc
        time.sleep(1)

print(f"ERROR: Prefect API did not become reachable at {api_url} within {wait_seconds}s", file=sys.stderr)
if last_error is not None:
    print(f"Last connection error: {last_error}", file=sys.stderr)
sys.exit(1)
PY
}

if [[ ! -d "${APP_DIR}" ]]; then
  echo "ERROR: APP_DIR does not exist: ${APP_DIR}" >&2
  exit 1
fi

if [[ "${GIT_PULL}" == "1" ]]; then
  run_as_user "cd '${APP_DIR}' && git fetch origin '${BRANCH}' && git reset --hard 'origin/${BRANCH}'"
fi

run_as_user "
  set -euo pipefail
  cd '${APP_DIR}'
  if [[ ! -d .venv ]]; then
    python3 -m venv .venv
  fi
  . .venv/bin/activate
  pip install -U pip
  pip install -r requirements.txt
"

systemctl_cmd restart cnc-prefect-server
wait_for_prefect_api "${PREFECT_API_URL}" "${PREFECT_API_WAIT_SECONDS}"

run_as_user "
  set -euo pipefail
  cd '${APP_DIR}'
  . .venv/bin/activate
  export PREFECT_HOME='${PREFECT_HOME}'
  export PREFECT_API_URL='${PREFECT_API_URL}'
  prefect --no-prompt work-pool create cnc-etl -t process || true
  prefect --no-prompt deploy --all
"

systemctl_cmd restart cnc-prefect-worker

echo "OK: deployed ${BRANCH} to ${APP_DIR}"
