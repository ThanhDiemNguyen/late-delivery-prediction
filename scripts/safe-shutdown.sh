#!/usr/bin/env bash
# Stops the platform and ejects the external SSD that holds Docker Desktop's disk image.
# It runs the steps of the safe stop procedure in docs/environment.md, in order, and stops
# before ejecting if any step fails, so the drive is never ejected while something uses it.
#
# Usage: scripts/safe-shutdown.sh [--yes]
#   --yes   skip the question "is every job finished?"
# The drive is /Volumes/T9 unless SSD_VOLUME is set.
set -euo pipefail

SSD_VOLUME="${SSD_VOLUME:-/Volumes/T9}"
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ASSUME_YES=false

# Print a step heading.
step() { printf '\n==> %s\n' "$*"; }

# Print an error and exit with status 1; the drive is left mounted.
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

# Succeed while the Docker engine answers.
docker_up() { docker info >/dev/null 2>&1; }

# Succeed while the Docker Desktop app is running.
docker_desktop_running() { pgrep -f "Docker Desktop.app/Contents/MacOS/Docker Desktop" >/dev/null; }

# Print the open files on the drive, one line per process and file.
ssd_users() { lsof 2>/dev/null | grep -F "$SSD_VOLUME" || true; }

# Print the physical disk (for example disk4) behind the volume. An APFS volume sits on a
# synthesized disk, so ejecting the volume alone would leave the physical disk attached.
physical_disk() {
  local store
  store=$(diskutil info "$SSD_VOLUME" | awk -F': *' '/APFS Physical Store/ {print $2}')
  diskutil info "${store:-$SSD_VOLUME}" | awk -F': *' '/Part of Whole/ {print $2}'
}

for arg in "$@"; do
  case "$arg" in
    -y|--yes) ASSUME_YES=true ;;
    -h|--help) sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown option: $arg (see --help)" ;;
  esac
done

[[ -d "$SSD_VOLUME" ]] || die "$SSD_VOLUME is not mounted, so there is nothing to eject"
cd "$REPO_DIR"

step "1/5 Check that no job is writing"
if docker_up; then
  docker ps --format '  {{.Names}}  {{.Status}}'
fi
if ! $ASSUME_YES; then
  read -r -p "Is every generator, Spark, Airflow and Flink job finished? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || die "nothing was stopped"
fi

step "2/5 Stop every container, with a 60 s grace period each"
if docker_up; then
  running=$(docker compose --profile "*" ps --services --status running)
  docker compose --profile "*" stop -t 60
  for service in $running; do
    code=$(docker compose --profile "*" ps -a --format '{{.ExitCode}}' "$service")
    printf '  %-16s exit %s\n' "$service" "$code"
    [[ "$code" != 137 ]] || echo "  WARNING: $service was killed after the grace period"
  done
  others=$(docker ps -q)
  if [[ -n "$others" ]]; then
    echo "  Stopping containers from other projects: $(docker ps --format '{{.Names}}' | tr '\n' ' ')"
    # shellcheck disable=SC2086  # one argument per container ID
    docker stop -t 60 $others >/dev/null
  fi
else
  echo "  Docker is not running"
fi

step "3/5 Quit Docker Desktop"
if docker_desktop_running; then
  osascript -e 'quit app "Docker Desktop"'
  for _ in $(seq 1 60); do
    docker_desktop_running || break
    sleep 2
  done
  ! docker_desktop_running || die "Docker Desktop is still running after 120 s; quit it from the menu bar"
fi
echo "  Docker Desktop is not running"

step "4/5 Check that nothing still uses $SSD_VOLUME"
cd /
users=""
for _ in $(seq 1 15); do
  users=$(ssd_users)
  [[ -n "$users" ]] || break
  sleep 2
done
if [[ -n "$users" ]]; then
  echo "$users" | awk '{print "  " $1 " (pid " $2 ")"}' | sort -u
  die "the drive is still in use; close the programs above, then run this script again"
fi
echo "  $SSD_VOLUME is not in use"

step "5/5 Eject $SSD_VOLUME"
disk=$(physical_disk)
[[ -n "$disk" ]] || die "could not find the physical disk behind $SSD_VOLUME"
diskutil eject "$disk"
echo "  Safe to unplug."
