# cnc-etl (Prefect)

This repo contains Prefect 3 flows for the City Nature Challenge ETL.

## Prereqs

- Python + a virtualenv with dependencies installed
- A `.env` file in the repo root (loaded by `settings.py`) containing:
  - `DO_SPACES_KEY=...`
  - `DO_SPACES_SECRET=...`
  - (optional) `DO_SPACES_BUCKET=cnc-assets`
  - (optional) `DO_SPACES_ENDPOINT=https://sfo3.digitaloceanspaces.com`
  - (optional) `DO_SPACES_REGION=sfo3`
  - (optional) `YEAR=2026`

## Local dashboard + worker (recommended)

### 1) Start the Prefect server + UI

Terminal 1:

```bash
cd /path/to/cnc-etl
source .venv/bin/activate
prefect server start
```

Terminal 2:

```bash
cd /path/to/cnc-etl
source .venv/bin/activate
prefect dashboard open
```

If your CLI falls back to a “temporary server” instead of using the server you
started on port 4200, force it with:

```bash
export PREFECT_API_URL=http://127.0.0.1:4200/api
```

### 2) Create a work pool (one-time)

```bash
cd /path/to/cnc-etl
source .venv/bin/activate
prefect work-pool create cnc-etl -t process
```

### 3) Deploy the flows (from `prefect.yaml`)

```bash
cd /path/to/cnc-etl
source .venv/bin/activate
prefect deploy --all
```

### 4) Start a worker (keep running)

Run the worker from the repo root so the deployments’ `working_dir: "."` resolves correctly.

```bash
cd /path/to/cnc-etl
source .venv/bin/activate
prefect worker start -p cnc-etl -t process
```

### 5) Run from the dashboard

In the UI: **Deployments** → pick one of:

- `update_umbrella_stats / umbrella`
- `update_identifiers_count / identifiers-count`
- `update_quality_grades / quality-grades`
- `update_most_observed_species / most-observed-species`
- `compose_city_results / compose`

Click **Run** and set parameters if you want (e.g. `year`, `project_id`, `api_call_delay`).

## Running from CLI (optional)

Once deployed and with a worker running:

```bash
cd /path/to/cnc-etl
source .venv/bin/activate

prefect deployment run update_umbrella_stats/umbrella --param year=2026
prefect deployment run update_identifiers_count/identifiers-count --param year=2026 --param api_call_delay=5.0
prefect deployment run update_quality_grades/quality-grades --param year=2026 --param api_call_delay=5.0
prefect deployment run update_most_observed_species/most-observed-species --param year=2026 --param api_call_delay=5.0
prefect deployment run compose_city_results/compose
```

## Prefect Variable for year

The ETL flows can read the target year from a Prefect Variable named
`cnc_year`. If the variable is unset or Prefect is unavailable, they fall back
to `YEAR` from `.env` / `settings.py`.

Set it from the server host with:

```bash
cd /srv/cnc-etl
source .venv/bin/activate
export PREFECT_API_URL=http://127.0.0.1:4200/api
prefect variable set cnc_year 2026
```

You can also create or edit `cnc_year` in the Prefect UI under **Variables**.

## Prefect Variables for API call delay

These flows can also read `api_call_delay` from separate Prefect Variables when
you do not pass the parameter explicitly:

- `update_identifiers_count`: `cnc_identifiers_count_api_call_delay`
- `update_quality_grades`: `cnc_quality_grades_api_call_delay`
- `update_most_observed_species`: `cnc_most_observed_species_api_call_delay`

If a variable is unset, invalid, or Prefect is unavailable, the flow falls back
to `3.0` seconds.

## Prefect Variables for rate-limit retries

The shared iNaturalist/HTTP retry helpers can also read these Prefect Variables:

- `cnc_rate_limit_max_retries`
- `cnc_rate_limit_backoff_factor`
- `cnc_rate_limit_min_retry_delay_seconds`
- `cnc_rate_limit_max_retry_delay_seconds`

Defaults:

- `max_retries = 5`
- `backoff_factor = 1.0`
- `min_retry_delay_seconds = 5.0`
- `max_retry_delay_seconds = 60.0`

If a variable is unset, invalid, or Prefect is unavailable, the code falls back
to those defaults.

## Running on a server

The pattern is the same:

1. Install deps + clone the repo to a fixed directory (e.g. `/srv/cnc-etl`).
2. Run `prefect server start` somewhere (or use Prefect Cloud) and point `PREFECT_API_URL` at it.
3. Run a long-lived worker from the repo directory (or set the service `WorkingDirectory` to the repo root).
4. Run `prefect deploy --all` after code/config changes.

Important: deployments reference the flow code on the machine that will execute
them. The simplest setup is to run `prefect deploy --all` on the same server
where the `prefect worker start ...` process will run (so the flow file paths
exist on that host).

### Single Droplet (systemd) recipe

This is the simplest “self-host everything on one server” setup: Prefect Server
and a `process` worker on the same Droplet.

1. Provision the Droplet (Ubuntu example)

```bash
sudo adduser --system --group --home /srv/cnc-etl cnc
sudo mkdir -p /srv/cnc-etl /var/lib/prefect
sudo chown -R cnc:cnc /srv/cnc-etl /var/lib/prefect

sudo apt-get update
sudo apt-get install -y git python3 python3-venv
```

2. Clone + install deps

```bash
sudo -u cnc -H bash -lc '
  cd /srv
  git clone <YOUR_GIT_URL> cnc-etl
  cd /srv/cnc-etl
  python3 -m venv .venv
  . .venv/bin/activate
  pip install -U pip
  pip install -r requirements.txt
'
```

3. Add secrets config

Create `/srv/cnc-etl/.env` (owned by `cnc:cnc`, mode `0600`) with at least:

```text
DO_SPACES_KEY=...
DO_SPACES_SECRET=...
```

4. Install + start services

```bash
sudo cp /srv/cnc-etl/deploy/systemd/cnc-prefect-server.service /etc/systemd/system/
sudo cp /srv/cnc-etl/deploy/systemd/cnc-prefect-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now cnc-prefect-server
```

If the server logs show `sqlite3.OperationalError: database is locked`, switch
Prefect Server to PostgreSQL. Prefect's self-hosted docs recommend Postgres for
production use, and it avoids the SQLite locking issue.

Install PostgreSQL and create the Prefect database:

```bash
sudo apt-get install -y postgresql postgresql-contrib
sudo -u postgres psql -c "CREATE USER prefect WITH PASSWORD 'replace-this-password';"
sudo -u postgres psql -c "CREATE DATABASE prefect OWNER prefect;"
sudo -u postgres psql -d prefect -c "CREATE EXTENSION IF NOT EXISTS pg_trgm;"
```

Create a root-managed env file for the systemd services:

```bash
sudo install -d -m 0755 /etc/cnc-etl
sudo install -m 0600 /dev/null /etc/cnc-etl/prefect.env
sudo editor /etc/cnc-etl/prefect.env
```

Put this in `/etc/cnc-etl/prefect.env`:

```bash
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://prefect:replace-this-password@127.0.0.1:5432/prefect
PREFECT_API_URL=http://127.0.0.1:4200/api
```

Then reload and restart the services:

```bash
sudo cp /srv/cnc-etl/deploy/systemd/cnc-prefect-server.service /etc/systemd/system/
sudo cp /srv/cnc-etl/deploy/systemd/cnc-prefect-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl restart cnc-prefect-server
sudo systemctl restart cnc-prefect-worker
```

One-time, create the work pool and deploy the flows (run as `cnc`):

```bash
sudo -u cnc -H bash -lc '
  cd /srv/cnc-etl
  . .venv/bin/activate
  export PREFECT_API_URL=http://127.0.0.1:4200/api
  prefect work-pool create cnc-etl -t process || true
  prefect deploy --all
'
sudo systemctl enable --now cnc-prefect-worker
```

5. Open the UI

Recommended: SSH tunnel to keep the API/UI private:

```bash
ssh -L 4200:127.0.0.1:4200 <user>@<droplet-ip>
```

Then visit `http://127.0.0.1:4200`.

### GitHub → Droplet pipeline (recommended)

Fastest/least-surprising pipeline: a GitHub Action SSHes into the Droplet and
runs `/srv/cnc-etl/deploy/remote_deploy.sh` (which does `git pull`, installs
deps, runs `prefect deploy --all`, and restarts the worker).

1. On the Droplet, create a login user for GitHub Actions (example `deployer`)

```bash
sudo adduser --disabled-password --gecos "" deployer
sudo mkdir -p /home/deployer/.ssh
sudo chown -R deployer:deployer /home/deployer/.ssh
sudo chmod 700 /home/deployer/.ssh
```

Add your public key to `/home/deployer/.ssh/authorized_keys`.

2. Allow deployer to run just the deploy script via sudo (no password)

```bash
sudo tee /etc/sudoers.d/cnc-etl-deploy >/dev/null <<'EOF'
deployer ALL=(root) NOPASSWD: /srv/cnc-etl/deploy/remote_deploy.sh
EOF
sudo chmod 440 /etc/sudoers.d/cnc-etl-deploy
```

3. Ensure `/srv/cnc-etl` can `git fetch` (deploy key)

Create an SSH key for the `cnc` user and add it to GitHub as a **read-only**
deploy key for the repo:

```bash
sudo -u cnc -H ssh-keygen -t ed25519 -N '' -f /srv/cnc-etl/.ssh/id_ed25519
sudo -u cnc -H bash -lc 'ssh-keyscan -H github.com >> /srv/cnc-etl/.ssh/known_hosts'
sudo chown -R cnc:cnc /srv/cnc-etl/.ssh
sudo chmod 700 /srv/cnc-etl/.ssh
sudo chmod 600 /srv/cnc-etl/.ssh/id_ed25519
sudo chmod 644 /srv/cnc-etl/.ssh/id_ed25519.pub
```

4. In GitHub repo settings, add Actions secrets:

- `DROPLET_HOST` (e.g. `203.0.113.10`)
- `DROPLET_USER` (e.g. `deployer`)
- `DROPLET_SSH_KEY` (private key for `deployer`)
- `DROPLET_KNOWN_HOSTS` (output of `ssh-keyscan -H <droplet-ip>`)
- (optional) `DROPLET_PORT` (default `22`)

Then pushes to `main` will deploy via `.github/workflows/deploy-droplet.yml`.
