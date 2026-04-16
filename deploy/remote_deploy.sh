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

  export PREFECT_HOME='${PREFECT_HOME}'
  export PREFECT_API_URL='${PREFECT_API_URL}'

  prefect --no-prompt work-pool create cnc-etl -t process || true
"

systemctl_cmd restart cnc-prefect-server
sleep 2

run_as_user "
  set -euo pipefail
  cd '${APP_DIR}'
  . .venv/bin/activate
  export PREFECT_HOME='${PREFECT_HOME}'
  export PREFECT_API_URL='${PREFECT_API_URL}'
  prefect --no-prompt deploy --all
"

systemctl_cmd restart cnc-prefect-worker

echo "OK: deployed ${BRANCH} to ${APP_DIR}"
