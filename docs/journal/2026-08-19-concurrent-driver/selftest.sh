#!/bin/bash
S="$(cd "$(dirname "$0")" && pwd)"
WT=/Users/brian/workspace/brayniac/pelikan-cachers-test
run() {
  local ph=$1 mode=$2; shift 2
  local out=$S/st-$ph-$mode
  local extra=()
  [ "$mode" = broken ] && extra=(--broken)
  PROFILE=release bash "$S/runconc.sh" "$WT" pelikan-segcache 4 "$ph" "$out" "$@" "${extra[@]}" > "$out.run" 2>&1
  local rc=$?
  printf '%-8s %-8s verdict_rc=%s  ' "$ph" "$mode" "$rc"
  grep -oE '(keys_with_lost_updates|keys_with_stale_token_success|phantom_values|false_misses|value_mismatches|replace_resurrected_deleted_key)[= ][^ ]*' "$out.run" | tr '\n' ' '
  echo
}
for mode in clean broken; do
  echo "### mode=$mode"
  run incr  $mode --threads 16 --ops 400 --keys 4
  run cas   $mode --threads 16 --ops 300 --keys 4
  run mixed $mode --threads 16 --ops 400 --keys 4
  run ryw   $mode --threads 16 --ops 300
  run add   $mode --threads 12 --rounds 40
done
