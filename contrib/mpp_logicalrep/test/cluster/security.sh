#!/bin/bash
# What the module exposes, and to whom.
#
#   1  the source's connection string, password and all, is dispatched to the
#      segments in pg_subscription -- where only a superuser may read it
#   2  ... and is kept out of the CONNECT payload the driver dispatches on
#      every reconnect, because a server that logs statements logs their
#      bound parameters too
#   3  the copy transport's socket takes rows straight into a table and has no
#      authentication but the job name, so the job name is a secret: a
#      connection that does not open with it is dropped, and the files named
#      after it are the owner's alone
#   4  mpp_logicalrep.copy_bind_address says which interface that socket is on
set -u
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null
PORT=7100
while getopts "s:" opt; do
  case $opt in
    s) PORT=$OPTARG ;;
    *) echo "usage: $0 [-s <port>]   (one cluster: database postgres -> lrdst)"; exit 2 ;;
  esac
done
Q="psql -p $PORT -d postgres -Atq -v ON_ERROR_STOP=0"
D="psql -p $PORT -d lrdst -Atq -v ON_ERROR_STOP=0"
SLOT=o3_slot; PUB=o3_pub; SUB=o3_sub; TBL=o3_t; SECRET=SeKrEtPa55
SCRATCH=${SCRATCH:-${TMPDIR:-/tmp}}/o3_$$
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }
workers() { $D -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='mpp apply worker'"; }
quiesce() { for k in $(seq 1 60); do [ "$(workers)" = "0" ] && return 0; sleep 0.5; done; return 1; }
tgt_log() { ls -t "$($Q -c 'SHOW data_directory')"/log/*.csv | head -1; }

cleanup() {
  $D -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
  $D -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  quiesce
  for k in $(seq 1 20); do
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='$SLOT'" >/dev/null 2>&1
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name='$SLOT'" >/dev/null 2>&1
    [ "$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break
    sleep 0.5
  done
  $Q -c "DROP PUBLICATION IF EXISTS $PUB; DROP TABLE IF EXISTS $TBL" >/dev/null 2>&1
  $D -c "DROP TABLE IF EXISTS $TBL" >/dev/null 2>&1
  $Q -c "DROP ROLE IF EXISTS o3_nobody" >/dev/null 2>&1
  $D -c "DROP ROLE IF EXISTS o3_nobody" >/dev/null 2>&1
  rm -rf "$SCRATCH"
}

echo "==== cluster :$PORT, postgres -> lrdst"
echo "==== cleanup"; cleanup
mkdir -p "$SCRATCH"

$Q -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO $TBL SELECT g,'seed' FROM generate_series(1,3000) g"
$Q -c "CREATE PUBLICATION $PUB FOR TABLE $TBL"
$D -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "SELECT count(*) FROM gp_create_logical_replication_slot('$SLOT','pgoutput',false,true)" >/dev/null

# ------------------------------------------------------------------ phase 1+2
echo; echo "==== 1. the connection string, with a password in it"
LOG=$(tgt_log); MARK=$(wc -l < "$LOG")
$D -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$PORT dbname=postgres password=$SECRET' PUBLICATION $PUB WITH (slot_name='$SLOT')" 2>&1 | grep -E '^ERROR' | head -2
for k in $(seq 1 60); do [ "$($D -c "SELECT count(*) FROM $TBL")" = "3000" ] && break; sleep 1; done
[ "$($D -c "SELECT count(*) FROM $TBL")" = "3000" ] && ok "the subscription works with a password in its conninfo" \
  || fail "the initial copy did not finish"

$D -c "CREATE ROLE o3_nobody"
out=$($D -c "SET ROLE o3_nobody; SELECT subconninfo FROM pg_subscription" 2>&1)
echo "$out" | grep -qi 'permission denied' && ok "a non-superuser cannot read subconninfo" \
  || fail "subconninfo readable by a non-superuser: $out"
out=$($D -c "SET ROLE o3_nobody; SELECT subname FROM pg_subscription" 2>&1)
echo "$out" | grep -qi 'permission denied' \
  && fail "the whole catalog is denied, not just the secret: $out" \
  || ok "the rest of the catalog is still readable"
# and the same row on a segment, where CREATE SUBSCRIPTION dispatched it
SEGP=$($Q -c "SELECT port FROM gp_segment_configuration WHERE content=0 AND role='p'")
out=$(PGOPTIONS='-c gp_role=utility' psql -p "$SEGP" -d lrdst -Atq \
        -c "SET ROLE o3_nobody; SELECT subconninfo FROM pg_subscription" 2>&1)
echo "$out" | grep -qi 'permission denied' && ok "nor on a segment" || fail "readable on a segment: $out"

echo; echo "==== 2. and not in the payload the driver dispatches"
$Q -c "INSERT INTO $TBL SELECT g,'more' FROM generate_series(3001,3100) g" >/dev/null
for k in $(seq 1 60); do [ "$($D -c "SELECT count(*) FROM $TBL")" = "3100" ] && break; sleep 1; done
# the driver reconnects its readers on every recovery, and each CONNECT carries
# the per-segment conninfo; count both in what this test wrote to the log
NCONN=$(tail -n +$MARK "$LOG" | grep -c 'conninfo\.0=')
NSEC=$(tail -n +$MARK "$LOG" | grep -c "$SECRET")
NDDL=$(tail -n +$MARK "$LOG" | grep -c "CREATE SUBSCRIPTION $SUB")
echo "  CONNECT payloads logged: $NCONN; lines with the password: $NSEC; of those, the DDL itself: $NDDL"
[ "$NCONN" -gt 0 ] && ok "the payload is logged (log_statement is on here), so this is a real test" \
  || echo "  (statement logging is off on this cluster; the count below is trivially 0)"
[ "$NSEC" -le "$NDDL" ] \
  && ok "the password appears only in the statement the user typed, never in a payload" \
  || fail "the password is in $NSEC lines but only $NDDL are the user's own statement"

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. the copy transport's socket"
JOB=o3job_$$
PGDATA="$SCRATCH" cbdb_lr_copyhelper recv --job "$JOB" --seg 0 --clients 1 \
  --port-range 42700-42799 --bindaddr 127.0.0.1 > "$SCRATCH/out.bin" 2> "$SCRATCH/recv.err" &
HELPER=$!
for k in $(seq 1 100); do [ -s "$SCRATCH/gp_lr/$JOB.0.port" ] && break; sleep 0.1; done
HPORT=$(cat "$SCRATCH/gp_lr/$JOB.0.port" 2>/dev/null)
[ -n "$HPORT" ] && ok "the helper published port $HPORT" || fail "the helper did not publish a port"

PORTMODE=$(stat -c %a "$SCRATCH/gp_lr/$JOB.0.port" 2>/dev/null)
DIRMODE=$(stat -c %a "$SCRATCH/gp_lr" 2>/dev/null)
[ "$PORTMODE" = "600" ] && ok "the port file is 0600" || fail "the port file is $PORTMODE"
[ "$DIRMODE" = "700" ] && ok "its directory is 0700" || fail "the directory is $DIRMODE"

python3 - "$HPORT" "$JOB" <<'PY'
import socket, struct, sys
port, job = int(sys.argv[1]), sys.argv[2]
def frame(b): return struct.pack("!I", len(b)) + b
# a stranger: right port, wrong job
s = socket.create_connection(("127.0.0.1", port), 10)
s.sendall(frame(b"not-the-job") + frame(b"rows that must not land") + frame(b""))
s.close()
# the real sender
s = socket.create_connection(("127.0.0.1", port), 10)
s.sendall(frame(job.encode()) + frame(b"the real payload\n") + frame(b""))
s.recv(64)          # the peer closes when it is done
s.close()
PY
wait $HELPER; RC=$?
[ "$RC" = "0" ] && ok "the helper finished cleanly" || fail "the helper exited $RC: $(head -2 $SCRATCH/recv.err)"
grep -q "the real payload" "$SCRATCH/out.bin" && ok "the real sender's bytes came through" \
  || fail "the real sender's bytes are missing"
grep -q "must not land" "$SCRATCH/out.bin" \
  && fail "a connection with the wrong job was let through" \
  || ok "the stranger's bytes were dropped"
grep -qi "rejected a connection" "$SCRATCH/recv.err" && ok "and it said so" \
  || fail "no warning about the rejected connection: $(cat $SCRATCH/recv.err)"
DONEMODE=$(stat -c %a "$SCRATCH/gp_lr/$JOB.0.done" 2>/dev/null)
[ "$DONEMODE" = "600" ] && ok "the done file is 0600" || fail "the done file is $DONEMODE"

# the job name has to be worth guarding, so the module's own is random
JOBNAME=$(grep -oh "(job lr[0-9]*_[0-9]*_[0-9a-f]*)" "$LOG" | tail -1 | tr -d '()')
echo "  the module's own job name for this copy: ${JOBNAME:-<not logged>}"
HEX=${JOBNAME##*_}
[ ${#HEX} -ge 32 ] && ok "it carries ${#HEX} hex digits of randomness" \
  || fail "the job name is guessable: $JOBNAME"

# ------------------------------------------------------------------ phase 4
echo; echo "==== 4. copy_bind_address"
# it goes on the helper's command line, which a shell runs
out=$($D -c "ALTER SYSTEM SET mpp_logicalrep.copy_bind_address = 'x; rm -rf /'" 2>&1)
echo "$out" | grep -qi 'invalid value' && ok "a shell metacharacter is refused" \
  || fail "a shell metacharacter was accepted: $(echo "$out" | head -1)"
out=$($D -c "ALTER SYSTEM SET mpp_logicalrep.copy_bind_address = '127.0.0.1'" 2>&1)
echo "$out" | grep -qi 'ERROR' && fail "a plain address was refused: $out" \
  || ok "a plain address is accepted"
$D -c "ALTER SYSTEM RESET mpp_logicalrep.copy_bind_address" >/dev/null 2>&1
$D -c "SELECT pg_reload_conf()" >/dev/null 2>&1
[ "$($D -c "SHOW mpp_logicalrep.copy_bind_address")" = "" ] \
  && ok "and the default is every interface" || fail "left set to $($D -c "SHOW mpp_logicalrep.copy_bind_address")"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
