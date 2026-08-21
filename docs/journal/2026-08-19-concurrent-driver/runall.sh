#!/bin/bash
# Full concurrent matrix for one build. Normal load, sequential phases.
set -u
S="$(cd "$(dirname "$0")" && pwd)"
WT="$1"
LABEL="$2"
WORKERS="${3:-2}"
OD="$S/conc-$LABEL-w$WORKERS"
mkdir -p "$OD"

echo "################ $LABEL  workers=$WORKERS ################"

run() {
  local prod=$1 phase=$2; shift 2
  echo
  echo "=== phase=$phase product=$prod workers=$WORKERS ==="
  bash "$S/runconc.sh" "$WT" "$prod" "$WORKERS" "$phase" "$OD/$phase" "$@"
  echo "phase_rc=$?"
}

run pelikan-segcache ryw   --threads 32 --ops 1500
run pelikan-segcache mixed --threads 32 --ops 3000 --keys 8
run pelikan-segcache incr  --threads 32 --ops 1500 --keys 4
run pelikan-segcache cas   --threads 32 --ops 800  --keys 4
run pelikan-segcache add   --threads 24 --rounds 300
run pelikan-segcache flush --threads 16 --after 0.5
run pelikan-rds      resp  --threads 32 --ops 2000 --keys 8
echo
echo "################ END $LABEL w$WORKERS ################"
