#!/bin/bash
# Pelikan integration tests under CPU oversubscription.
#
# Safety: load generators are (a) tracked and killed by an EXIT/INT/TERM trap,
# and (b) self-limiting -- each one exits on its own after LOAD_MAX_SEC even if
# this script is SIGKILLed and the trap never runs.
set -u

S="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="${BIN_DIR:-$S/bin}"
TAG="${TAG:-run}"
LOG_DIR="$S/logs/$TAG"
NLOAD="${NLOAD:-256}"
ITERS="${ITERS:-40}"
PER_RUN_TIMEOUT="${PER_RUN_TIMEOUT:-90}"
LOAD_MAX_SEC="${LOAD_MAX_SEC:-5400}"

mkdir -p "$LOG_DIR"
SUMMARY="$LOG_DIR/summary.tsv"
: > "$SUMMARY"

LOADPIDS=""
cleanup() {
  if [ -n "$LOADPIDS" ]; then
    kill $LOADPIDS 2>/dev/null
    sleep 1
    kill -9 $LOADPIDS 2>/dev/null
  fi
  LOADPIDS=""
}
trap 'cleanup' EXIT INT TERM

start_load() {
  local n=$1
  local i
  for ((i = 0; i < n; i++)); do
    bash -c 'end=$((SECONDS+'"$LOAD_MAX_SEC"')); while : ; do [ $SECONDS -ge $end ] && break; done' &
    LOADPIDS="$LOADPIDS $!"
  done
  echo "started $n load generators (self-limit ${LOAD_MAX_SEC}s)"
}

if [ "$NLOAD" -gt 0 ]; then
  start_load "$NLOAD"
  sleep 3
fi
echo "load average at start: $(uptime)"

BINS="segcache-integration segcache-integration_multi rds-integration rds-integration_multi"

fails=0
hangs=0
total=0

for ((i = 1; i <= ITERS; i++)); do
  for b in $BINS; do
    log="$LOG_DIR/$b.$i.log"
    t0=$SECONDS
    timeout -k 5 "$PER_RUN_TIMEOUT" "$BIN_DIR/$b" > "$log" 2>&1
    rc=$?
    dt=$((SECONDS - t0))
    total=$((total + 1))
    status=ok
    if [ "$rc" -eq 124 ] || [ "$rc" -eq 137 ]; then
      status=HANG
      hangs=$((hangs + 1))
    elif [ "$rc" -ne 0 ]; then
      status=FAIL
      fails=$((fails + 1))
    else
      rm -f "$log"
    fi
    printf '%s\t%d\t%s\t%d\t%ds\n' "$b" "$i" "$status" "$rc" "$dt" >> "$SUMMARY"
    if [ "$status" != ok ]; then
      echo "iter $i $b -> $status rc=$rc ${dt}s"
    fi
    sleep 1
  done
  if (( i % 5 == 0 )); then
    echo "[$(date +%H:%M:%S)] iter $i/$ITERS  total=$total fails=$fails hangs=$hangs  $(uptime | sed 's/.*load/load/')"
  fi
done

echo "=== DONE tag=$TAG nload=$NLOAD iters=$ITERS total=$total fails=$fails hangs=$hangs"
cleanup
sleep 1
echo "remaining load generators: $(pgrep -f 'while :' | wc -l | tr -d ' ')"
