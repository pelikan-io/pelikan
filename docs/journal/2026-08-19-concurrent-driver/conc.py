#!/usr/bin/env python3
"""Concurrent multi-connection driver for pelikan's shared-Arc<Seg> workers.

Why this exists: pelikan's `integration_multi` test starts N workers but opens
ONE connection at a time, so exactly one worker ever handles traffic. The
N-worker shared-storage claim the concurrent-segcache branch exists to deliver
has therefore never been exercised. This driver puts genuinely concurrent
traffic from many connections onto the same keys so the workers contend on one
`Arc<Seg>`.

Every phase is an INVARIANT with a defined verdict, so an observation can be
classified as:
  ACCEPTED - documented check-then-act race (concurrent `add` both storing)
  BUG      - lost update, phantom value, stale-token cas, false miss, hang

Usage: conc.py <phase> [options]
"""
import argparse
import random
import socket
import sys
import threading
import time
from collections import defaultdict

HOST = "127.0.0.1"


class Conn:
    """Line-oriented client connection."""

    def __init__(self, port, timeout=15):
        self.s = socket.create_connection((HOST, port), timeout=timeout)
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.f = self.s.makefile("rwb")

    def cmd(self, data):
        self.f.write(data)
        self.f.flush()

    def line(self):
        ln = self.f.readline()
        if not ln:
            raise EOFError("server closed connection")
        return ln

    def close(self):
        try:
            self.f.close()
            self.s.close()
        except Exception:
            pass


# ---------------------------------------------------------------- memcache ---

def mc_get(c, key):
    """Return (value_bytes | None). Raises on protocol surprise."""
    c.cmd(b"get %s\r\n" % key)
    ln = c.line()
    if ln == b"END\r\n":
        return None
    if not ln.startswith(b"VALUE "):
        raise ValueError("unexpected get response: %r" % ln)
    nbytes = int(ln.split()[3])
    body = c.f.read(nbytes + 2)[:nbytes]
    end = c.line()
    if end != b"END\r\n":
        raise ValueError("missing END: %r" % end)
    return body


def mc_gets(c, key):
    """Return (value_bytes, cas_token) | (None, None)."""
    c.cmd(b"gets %s\r\n" % key)
    ln = c.line()
    if ln == b"END\r\n":
        return None, None
    if not ln.startswith(b"VALUE "):
        raise ValueError("unexpected gets response: %r" % ln)
    parts = ln.split()
    nbytes = int(parts[3])
    token = parts[4]
    body = c.f.read(nbytes + 2)[:nbytes]
    end = c.line()
    if end != b"END\r\n":
        raise ValueError("missing END: %r" % end)
    return body, token


def mc_store(c, verb, key, val, extra=b""):
    c.cmd(b"%s %s 0 0 %d%s\r\n%s\r\n" % (verb, key, len(val), extra, val))
    return c.line()


def mc_incr(c, key, n):
    c.cmd(b"incr %s %d\r\n" % (key, n))
    return c.line()


def mc_decr(c, key, n):
    c.cmd(b"decr %s %d\r\n" % (key, n))
    return c.line()


def mc_delete(c, key):
    c.cmd(b"delete %s\r\n" % key)
    return c.line()


# -------------------------------------------------------------------- resp ---

def resp_set(c, key, val):
    c.cmd(b"set %s %s\r\n" % (key, val))
    return c.line()


def resp_get(c, key):
    c.cmd(b"get %s\r\n" % key)
    ln = c.line()
    if ln.startswith(b"$-1"):
        return None
    if not ln.startswith(b"$"):
        raise ValueError("unexpected resp get: %r" % ln)
    n = int(ln[1:].strip())
    body = c.f.read(n + 2)[:n]
    return body


# ------------------------------------------------------------------ phases ---

def phase_incr(args):
    """Lost-update detector on the numeric path (segcache numeric_update)."""
    port = args.port
    keys = [b"ctr:%d" % i for i in range(args.keys)]
    c0 = Conn(port)
    for k in keys:
        mc_store(c0, b"set", k, b"0")
    c0.close()

    ok = defaultdict(int)
    lock = threading.Lock()
    errors = []

    def worker(tid):
        c = Conn(port)
        local = defaultdict(int)
        try:
            for _ in range(args.ops):
                k = random.choice(keys)
                r = mc_incr(c, k, 1)
                if r.strip().isdigit():
                    local[k] += 1
                else:
                    with lock:
                        errors.append(("incr", r))
        except Exception as e:
            with lock:
                errors.append(("exc", repr(e)))
        finally:
            c.close()
        with lock:
            for k, v in local.items():
                ok[k] += v

    run_threads(worker, args.threads)

    c0 = Conn(port)
    lost = 0
    detail = []
    for k in keys:
        v = mc_get(c0, k)
        got = int(v) if v is not None else None
        exp = ok[k]
        if got != exp:
            lost += 1
            detail.append((k.decode(), exp, got))
    c0.close()
    print(f"[incr] threads={args.threads} ops={args.ops} keys={args.keys} "
          f"successful_incrs={sum(ok.values())}")
    print(f"[incr] keys_with_lost_updates={lost} protocol_errors={len(errors)}")
    for d in detail[:5]:
        print(f"   BUG lost-update key={d[0]} expected={d[1]} actual={d[2]}")
    for e in errors[:5]:
        print("   err:", e)
    return 1 if (lost or errors) else 0


def phase_cas(args):
    """Stale-token detector: final value must equal count of successful cas."""
    port = args.port
    keys = [b"cas:%d" % i for i in range(args.keys)]
    c0 = Conn(port)
    for k in keys:
        mc_store(c0, b"set", k, b"0")
    c0.close()

    stored = defaultdict(int)
    lock = threading.Lock()
    errors = []
    exists = [0]

    def worker(tid):
        c = Conn(port)
        local = defaultdict(int)
        nex = 0
        try:
            for _ in range(args.ops):
                k = random.choice(keys)
                v, tok = mc_gets(c, k)
                if v is None:
                    with lock:
                        errors.append(("gets-miss", k))
                    continue
                nv = b"%d" % (int(v) + 1)
                r = mc_store(c, b"cas", k, nv, extra=b" %s" % tok.decode().encode())
                if r == b"STORED\r\n":
                    local[k] += 1
                elif r == b"EXISTS\r\n":
                    nex += 1
                else:
                    with lock:
                        errors.append(("cas", r))
        except Exception as e:
            with lock:
                errors.append(("exc", repr(e)))
        finally:
            c.close()
        with lock:
            exists[0] += nex
            for k, v in local.items():
                stored[k] += v

    run_threads(worker, args.threads)

    c0 = Conn(port)
    bad = []
    for k in keys:
        v = mc_get(c0, k)
        got = int(v) if v is not None else None
        if got != stored[k]:
            bad.append((k.decode(), stored[k], got))
    c0.close()
    print(f"[cas] threads={args.threads} ops={args.ops} keys={args.keys} "
          f"cas_stored={sum(stored.values())} cas_exists={exists[0]}")
    print(f"[cas] keys_with_stale_token_success={len(bad)} protocol_errors={len(errors)}")
    for d in bad[:5]:
        print(f"   BUG cas-lost-update key={d[0]} successful_cas={d[1]} final_value={d[2]}")
    for e in errors[:5]:
        print("   err:", e)
    return 1 if (bad or errors) else 0


def phase_add(args):
    """Documented check-then-act race: concurrent add on one fresh key."""
    port = args.port
    conns = [Conn(port) for _ in range(args.threads)]
    multi_stored = 0
    phantom = 0
    rounds = args.rounds
    barrier = threading.Barrier(args.threads)
    results = [None] * args.threads
    replace_multi = 0

    for r in range(rounds):
        key = b"addk:%d" % r
        mc_delete(conns[0], key)

        def worker(tid):
            barrier.wait()
            val = b"v%03d" % tid
            resp = mc_store(conns[tid], b"add", key, val)
            results[tid] = (resp, val)

        run_threads(worker, args.threads, join=True)
        winners = [v for (resp, v) in results if resp == b"STORED\r\n"]
        if len(winners) > 1:
            multi_stored += 1
        final = mc_get(conns[0], key)
        if final is not None and final not in [v for (_, v) in results]:
            phantom += 1

    # `replace` on a key that EXISTS must succeed for every caller -- that is
    # its definition, not a race -- so racing replaces on a live key measures
    # nothing. The meaningful check is the negative side of the check-then-act:
    # against an ABSENT key every concurrent `replace` must return NOT_STORED.
    # A STORED here would mean replace resurrected a deleted key.
    resurrect = 0
    for r in range(rounds):
        key = b"repk:%d" % r
        mc_store(conns[0], b"set", key, b"init")
        mc_delete(conns[0], key)

        def worker2(tid):
            barrier.wait()
            val = b"r%03d" % tid
            resp = mc_store(conns[tid], b"replace", key, val)
            results[tid] = (resp, val)

        run_threads(worker2, args.threads, join=True)
        wins = [v for (resp, v) in results if resp == b"STORED\r\n"]
        if len(wins) > 0:
            resurrect += 1
            replace_multi += 1
        final = mc_get(conns[0], key)
        if final is not None:
            phantom += 1

    for c in conns:
        c.close()
    print(f"[add] rounds={rounds} concurrency={args.threads}")
    print(f"[add] ACCEPTED multi-STORED add rounds={multi_stored}/{rounds} "
          f"({100.0*multi_stored/rounds:.1f}%)")
    print(f"[add] BUG replace_resurrected_deleted_key rounds={resurrect}/{rounds}")
    print(f"[add] BUG phantom_final_values={phantom}")
    return 1 if phantom else 0


def phase_mixed(args):
    """Phantom-value and false-miss detector on hot contended keys."""
    port = args.port
    hot = [b"hot:%d" % i for i in range(args.keys)]
    ever = defaultdict(set)
    lock = threading.Lock()
    phantom = []
    errors = []
    counts = defaultdict(int)

    def worker(tid):
        c = Conn(port)
        local_phantom = []
        local_err = []
        local_counts = defaultdict(int)
        try:
            for i in range(args.ops):
                k = random.choice(hot)
                op = random.random()
                if op < 0.40:
                    val = b"t%03d-s%06d" % (tid, i)
                    with lock:
                        ever[k].add(val)
                    r = mc_store(c, b"set", k, val)
                    local_counts["set" if r == b"STORED\r\n" else "set_other"] += 1
                elif op < 0.50:
                    val = b"T%03d-S%06d" % (tid, i)
                    with lock:
                        ever[k].add(val)
                    r = mc_store(c, b"replace", k, val)
                    local_counts["replace_stored" if r == b"STORED\r\n"
                                 else "replace_not_stored"] += 1
                elif op < 0.55:
                    mc_delete(c, k)
                    local_counts["delete"] += 1
                else:
                    v = mc_get(c, k)
                    if v is None:
                        local_counts["get_miss"] += 1
                    else:
                        local_counts["get_hit"] += 1
                        with lock:
                            known = v in ever[k]
                        if not known:
                            local_phantom.append((k, v))
        except Exception as e:
            local_err.append(("exc", repr(e)))
        finally:
            c.close()
        with lock:
            phantom.extend(local_phantom)
            errors.extend(local_err)
            for kk, vv in local_counts.items():
                counts[kk] += vv

    run_threads(worker, args.threads)
    print(f"[mixed] threads={args.threads} ops={args.ops} hot_keys={args.keys}")
    print("[mixed] " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print(f"[mixed] BUG phantom_values={len(phantom)} protocol_errors={len(errors)}")
    for p in phantom[:5]:
        print(f"   BUG phantom key={p[0]!r} value={p[1]!r}")
    for e in errors[:5]:
        print("   err:", e)
    return 1 if (phantom or errors) else 0


def phase_ryw(args):
    """Read-your-writes on private keys while other threads churn the cache.

    A miss or mismatch here cannot be an eviction (heap is large relative to
    the working set) so it would be a false miss on the read path -- exactly
    what #60/#68 address.
    """
    port = args.port
    misses = []
    mismatches = []
    errors = []
    lock = threading.Lock()
    total = [0]

    def worker(tid):
        c = Conn(port)
        lm, lx, le = [], [], []
        n = 0
        try:
            for i in range(args.ops):
                k = b"priv:%03d:%06d" % (tid, i % 64)
                val = b"p%03d-%08d" % (tid, i)
                r = mc_store(c, b"set", k, val)
                if r != b"STORED\r\n":
                    le.append(("set", r))
                    continue
                got = mc_get(c, k)
                n += 1
                if got is None:
                    lm.append((k, val))
                elif got != val:
                    lx.append((k, val, got))
        except Exception as e:
            le.append(("exc", repr(e)))
        finally:
            c.close()
        with lock:
            misses.extend(lm)
            mismatches.extend(lx)
            errors.extend(le)
            total[0] += n

    def churner(tid):
        c = Conn(port)
        try:
            for i in range(args.ops):
                k = b"churn:%03d:%08d" % (tid, i)
                mc_store(c, b"set", k, b"c" * 512)
        except Exception:
            pass
        finally:
            c.close()

    ths = []
    for t in range(args.threads):
        ths.append(threading.Thread(target=worker, args=(t,)))
    for t in range(max(2, args.threads // 2)):
        ths.append(threading.Thread(target=churner, args=(t,)))
    for t in ths:
        t.start()
    for t in ths:
        t.join()

    print(f"[ryw] threads={args.threads} verified_reads={total[0]}")
    print(f"[ryw] BUG false_misses={len(misses)} value_mismatches={len(mismatches)} "
          f"protocol_errors={len(errors)}")
    for m in misses[:5]:
        print(f"   BUG false-miss key={m[0]!r} expected={m[1]!r}")
    for m in mismatches[:5]:
        print(f"   BUG mismatch key={m[0]!r} expected={m[1]!r} got={m[2]!r}")
    for e in errors[:5]:
        print("   err:", e)
    return 1 if (misses or mismatches or errors) else 0


def phase_flush(args):
    """Characterise the flush_all smear window with writes in flight.

    Documented accepted race: the admin thread broadcasts flush_all and each
    worker calls clear(); a write acked between the FIRST and LAST worker's
    clear can be destroyed by the later duplicate clear. This measures how wide
    that window actually is, in time and in acked writes.
    """
    port = args.port
    stop = threading.Event()
    acked = []  # (monotonic_ack_time, key)
    lock = threading.Lock()

    def writer(tid):
        c = Conn(port)
        local = []
        i = 0
        try:
            while not stop.is_set():
                k = b"fl:%03d:%08d" % (tid, i)
                r = mc_store(c, b"set", k, b"x")
                if r == b"STORED\r\n":
                    local.append((time.monotonic(), k))
                i += 1
        except Exception:
            pass
        finally:
            c.close()
        with lock:
            acked.extend(local)

    ths = [threading.Thread(target=writer, args=(t,)) for t in range(args.threads)]
    for t in ths:
        t.start()
    time.sleep(0.5)

    a = Conn(args.admin_port)
    t_sent = time.monotonic()
    a.cmd(b"flush_all\r\n")
    resp = a.line()
    t_ok = time.monotonic()
    a.close()

    time.sleep(args.after)
    stop.set()
    for t in ths:
        t.join()

    with lock:
        acked.sort()
    c = Conn(port)
    survived, destroyed = [], []
    # only inspect writes acked from shortly before the flush onward
    window = [(t, k) for (t, k) in acked if t >= t_sent - 0.25]
    for t, k in window:
        if mc_get(c, k) is None:
            destroyed.append((t, k))
        else:
            survived.append((t, k))
    c.close()

    pre = [t for (t, _) in acked if t < t_sent]
    pre_destroyed = 0
    cpre = Conn(port)
    sample = [k for (t, k) in acked if t < t_sent][-200:]
    for k in sample:
        if mc_get(cpre, k) is None:
            pre_destroyed += 1
    cpre.close()

    post_destroyed = [t for (t, _) in destroyed if t > t_ok]
    last_destroyed = max((t for (t, _) in destroyed), default=None)

    print(f"[flush] writers={args.threads} admin_response={resp!r} "
          f"admin_rtt_ms={(t_ok-t_sent)*1000:.2f}")
    print(f"[flush] acked_writes_total={len(acked)} acked_before_flush={len(pre)}")
    print(f"[flush] pre-flush sample destroyed {pre_destroyed}/{len(sample)} (EXPECTED: all)")
    print(f"[flush] acked_after_flush_sent={len(window)} destroyed={len(destroyed)} "
          f"survived={len(survived)}")
    print(f"[flush] acked AFTER admin OK but destroyed = {len(post_destroyed)}"
          f"   <-- the smear beyond the ack")
    if last_destroyed is not None:
        print(f"[flush] SMEAR WINDOW: last destroyed ack at t_sent+"
              f"{(last_destroyed-t_sent)*1000:.2f}ms "
              f"(admin OK at t_sent+{(t_ok-t_sent)*1000:.2f}ms)")
    return 0


def phase_resp(args):
    """Concurrent RESP get/set: phantom-value and read-your-writes."""
    port = args.port
    hot = [b"rhot:%d" % i for i in range(args.keys)]
    ever = defaultdict(set)
    lock = threading.Lock()
    phantom, errors, ryw_miss, ryw_bad = [], [], [], []
    counts = defaultdict(int)

    def worker(tid):
        c = Conn(port)
        lp, le, lm, lb = [], [], [], []
        lc = defaultdict(int)
        try:
            for i in range(args.ops):
                if random.random() < 0.5:
                    k = random.choice(hot)
                    val = b"t%03ds%06d" % (tid, i)
                    with lock:
                        ever[k].add(val)
                    r = resp_set(c, k, val)
                    lc["set_ok" if r == b"+OK\r\n" else "set_other"] += 1
                    if r != b"+OK\r\n":
                        le.append(("set", r))
                    v = resp_get(c, k)
                    if v is not None:
                        with lock:
                            known = v in ever[k]
                        if not known:
                            lp.append((k, v))
                else:
                    k = b"rpriv:%03d:%06d" % (tid, i % 64)
                    val = b"p%03d%08d" % (tid, i)
                    r = resp_set(c, k, val)
                    if r != b"+OK\r\n":
                        le.append(("set", r))
                        continue
                    got = resp_get(c, k)
                    lc["ryw"] += 1
                    if got is None:
                        lm.append((k, val))
                    elif got != val:
                        lb.append((k, val, got))
        except Exception as e:
            le.append(("exc", repr(e)))
        finally:
            c.close()
        with lock:
            phantom.extend(lp)
            errors.extend(le)
            ryw_miss.extend(lm)
            ryw_bad.extend(lb)
            for kk, vv in lc.items():
                counts[kk] += vv

    run_threads(worker, args.threads)
    print(f"[resp] threads={args.threads} ops={args.ops} hot_keys={args.keys}")
    print("[resp] " + " ".join(f"{k}={v}" for k, v in sorted(counts.items())))
    print(f"[resp] BUG phantom_values={len(phantom)} false_misses={len(ryw_miss)} "
          f"mismatches={len(ryw_bad)} protocol_errors={len(errors)}")
    for p in phantom[:5]:
        print(f"   BUG phantom key={p[0]!r} value={p[1]!r}")
    for m in ryw_miss[:5]:
        print(f"   BUG false-miss key={m[0]!r} expected={m[1]!r}")
    for m in ryw_bad[:5]:
        print(f"   BUG mismatch key={m[0]!r} expected={m[1]!r} got={m[2]!r}")
    for e in errors[:5]:
        print("   err:", e)
    return 1 if (phantom or ryw_miss or ryw_bad or errors) else 0


def run_threads(fn, n, join=True):
    ths = [threading.Thread(target=fn, args=(i,)) for i in range(n)]
    for t in ths:
        t.start()
    if join:
        for t in ths:
            t.join()
    return ths


PHASES = {
    "incr": phase_incr,
    "cas": phase_cas,
    "add": phase_add,
    "mixed": phase_mixed,
    "ryw": phase_ryw,
    "flush": phase_flush,
    "resp": phase_resp,
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("phase", choices=sorted(PHASES))
    p.add_argument("--port", type=int, default=12321)
    p.add_argument("--admin-port", type=int, default=9999)
    p.add_argument("--threads", type=int, default=32)
    p.add_argument("--ops", type=int, default=2000)
    p.add_argument("--keys", type=int, default=8)
    p.add_argument("--rounds", type=int, default=200)
    p.add_argument("--after", type=float, default=0.5)
    p.add_argument("--seed", type=int, default=0)
    args = p.parse_args()
    random.seed(args.seed)
    t0 = time.time()
    rc = PHASES[args.phase](args)
    print(f"[{args.phase}] elapsed={time.time()-t0:.1f}s verdict_rc={rc}")
    return rc


if __name__ == "__main__":
    sys.exit(main())
