#!/bin/bash
# Launch a pelikan server with W workers and run one concurrent-driver phase.
# Also samples per-thread CPU so we can PROVE more than one worker was active,
# rather than assuming the listener's random session assignment spread them.
set -u
S="$(cd "$(dirname "$0")" && pwd)"
WT="$1"        # worktree
PROD="$2"      # pelikan-segcache | pelikan-rds
WORKERS="$3"
PHASE="$4"
OUT="$5"
shift 5
EXTRA=("$@")

CFG="$S/conc-$PROD-$WORKERS.toml"
cat > "$CFG" <<EOF
daemonize = false
[admin]
host = "127.0.0.1"
port = "9999"
http_enabled = true
http_host = "127.0.0.1"
http_port = "9998"
[server]
host = "127.0.0.1"
port = "12321"
timeout = 100
nevent = 1024
[worker]
timeout = 100
nevent = 1024
threads = $WORKERS
[seg]
hash_power = 20
heap_size = "${HEAP:-256MB}"
segment_size = "1MB"
compact_target = 2
merge_target = 4
merge_max = 8
eviction = "Merge"
[time]
time_type = "Memcache"
[buf]
[debug]
log_level = "warn"
[klog]
max_size = "1GB"
sample = 0
[sockio]
[tcp]
[tls]
EOF

SRV=""
cleanup() {
  if [ -n "$SRV" ]; then kill "$SRV" 2>/dev/null; sleep 1; kill -9 "$SRV" 2>/dev/null; fi
  SRV=""
}
trap cleanup EXIT INT TERM

"$WT/target/${PROFILE:-debug}/$PROD" "$CFG" > "$OUT.server.log" 2>&1 &
SRV=$!
sleep 4

if ! kill -0 "$SRV" 2>/dev/null; then
  echo "SERVER FAILED TO START"; cat "$OUT.server.log"; exit 1
fi

ps -M "$SRV" | awk 'NR>1{print $NF}' > "$OUT.threads.before" 2>/dev/null

python3 "$S/conc.py" "$PHASE" "${EXTRA[@]}" > "$OUT.txt" 2>&1
rc=$?

ps -M "$SRV" > "$OUT.threads.after" 2>/dev/null

cat "$OUT.txt"
# Prove worker spread: print per-thread CPU (STIME+UTIME) for the busiest
# threads. Sessions are handed to workers UNIFORMLY AT RANDOM (queues.rs
# try_send_any), so this is the empirical check that more than one worker
# actually ran, rather than an assumption.
echo "--- busiest server threads (sys+user seconds), workers=$WORKERS:"
awk 'NR>1 {
  tot=0
  for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+:[0-9][0-9]\.[0-9][0-9]$/) {
    split($i, a, ":"); tot += a[1]*60 + a[2]
  }
  printf "%.2f\n", tot
}' "$OUT.threads.after" 2>/dev/null | sort -rn | head -6 | awk '{printf "  thread cpu=%ss\n", $1}'
echo "--- server stderr/stdout (warn+):"
grep -vE '^\s*$' "$OUT.server.log" | tail -5
cleanup
exit $rc
