#!/usr/bin/env bash
# Watches the free disk space during the 100 GB runs and stops the jobs before the disk fills up.
# Free space is the smaller of two numbers: the external SSD that holds Docker Desktop's disk image
# (the image grows on it), and the Docker VM's own disk, whose size limit can be reached first.
# Below the threshold it kills every running Spark application through the master UI and stops the
# generator, then exits with status 2.
#
# Usage: scripts/disk-watch.sh [--threshold-gb N] [--interval S] [--once]
#   --threshold-gb N   stop the jobs below N GB free (default 50)
#   --interval S       seconds between checks (default 30)
#   --once             check once, stop the jobs if needed, and exit
# The drive is /Volumes/T9 unless SSD_VOLUME is set. GENERATOR_PATTERN is the pkill pattern (an
# extended regular expression) of the generator processes: the uv wrapper, its Python process and
# a shell that runs them. Set it to an empty string to leave the generator running.
set -euo pipefail

SSD_VOLUME="${SSD_VOLUME:-/Volumes/T9}"
SPARK_MASTER_UI="${SPARK_MASTER_UI:-http://127.0.0.1:8080}"
GENERATOR_PATTERN="${GENERATOR_PATTERN--m generator\.(oms_reference|oms_orders|app_exports)}"
THRESHOLD_GB=50
INTERVAL_S=30
ONCE=false

# Print an error and exit with status 1.
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# Print the free GB on the SSD.
ssd_free_gb() { df -k "$SSD_VOLUME" | awk 'NR == 2 { print int($4 / 1048576) }'; }

# Print the free GB on the Docker VM's disk, or nothing if no container can start.
vm_free_gb() { docker run --rm alpine:3 df -k / 2>/dev/null | awk 'NR == 2 { print int($4 / 1048576) }' || true; }

# Print the ids of the running Spark applications, one per line.
spark_apps() {
  curl -fsS "$SPARK_MASTER_UI/json/" 2>/dev/null \
    | python3 -c 'import json, sys; [print(a["id"]) for a in json.load(sys.stdin)["activeapps"]]' || true
}

# Kill every running Spark application and stop the generator.
stop_jobs() {
  local app
  for app in $(spark_apps); do
    curl -fsS -X POST "$SPARK_MASTER_UI/app/kill/?id=$app&terminate=true" >/dev/null && echo "killed Spark application $app"
  done
  if [[ -n "$GENERATOR_PATTERN" ]] && pkill -f "$GENERATOR_PATTERN"; then
    echo "stopped the generator ($GENERATOR_PATTERN)"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threshold-gb) THRESHOLD_GB="${2:?}"; shift 2 ;;
    --interval) INTERVAL_S="${2:?}"; shift 2 ;;
    --once) ONCE=true; shift ;;
    *) die "unknown option $1 (see the usage at the top of this script)" ;;
  esac
done
[[ -d "$SSD_VOLUME" ]] || die "$SSD_VOLUME is not mounted"

while true; do
  ssd=$(ssd_free_gb)
  vm=$(vm_free_gb)
  free=$ssd
  [[ -n "$vm" && "$vm" -lt "$free" ]] && free=$vm
  echo "$(date '+%Y-%m-%d %H:%M:%S') free: SSD ${ssd} GB, Docker VM ${vm:-unknown} GB; threshold ${THRESHOLD_GB} GB"
  if (( free < THRESHOLD_GB )); then
    echo "below ${THRESHOLD_GB} GB free: stopping the jobs"
    stop_jobs
    exit 2
  fi
  $ONCE && exit 0
  sleep "$INTERVAL_S"
done
