#!/bin/bash
# End-to-end replication between TWO clusters, with faults injected while a
# workload runs.  This is the test that exercises the whole module.
#
# Source  = a cluster with wal_level = logical (default :7100)
# Target  = a second cluster with the module preloaded (default :7300);
#           see README for how to make one with gpAux/gpdemo.
#
#   1  initial sync of a mixed schema (hash PK / random FULL / replicated / AO)
#      while a workload runs, then streaming
#   2  mixed workload: multi-row 2PC INSERT, one-phase single-row INSERT,
#      keyed UPDATE/DELETE, no-match UPDATE, savepoint rollback, big
#      (streamed) transaction, TRUNCATE
#   3  faults during the workload: reader QE killed, driven QD session killed,
#      worker SIGKILLed, source walsender killed
#   4  a table added to the running subscription (REFRESH PUBLICATION)
#   5  verification: per-table checksums, per-segment row counts, progress
#      state (all tables 'r', applied set trimmed, seg_pos == seg_flush)
#
# Usage: end_to_end.sh [-s <source port>] [-t <target port>] [-d <seconds>]
set -u
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null

SRC_PORT=7100; TGT_PORT=7300; LOAD_SECS=45
while getopts "s:t:d:" opt; do
  case $opt in
    s) SRC_PORT=$OPTARG ;; t) TGT_PORT=$OPTARG ;; d) LOAD_SECS=$OPTARG ;;
    *) echo "usage: $0 [-s src_port] [-t tgt_port] [-d load_seconds]"; exit 2 ;;
  esac
done

SRC_DB=postgres; TGT_DB=postgres
SLOT=r12_slot; PUB=r12_pub; SUB=r12_sub
S="psql -p $SRC_PORT -d $SRC_DB -Atq -v ON_ERROR_STOP=0"
T="psql -p $TGT_PORT -d $TGT_DB -Atq -v ON_ERROR_STOP=0"
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }

src_datadir() { psql -p $SRC_PORT -d $SRC_DB -Atq -c "SHOW data_directory"; }
tgt_datadir() { psql -p $TGT_PORT -d $TGT_DB -Atq -c "SHOW data_directory"; }

# order-independent, cluster-independent checksum of a whole table
ck() { psql -p "$1" -d "$2" -Atq -c "SELECT count(*)||'/'||coalesce(sum(hashtext(x::text))::bigint,0) FROM $3 x"; }
# per-segment counts (only comparable when both clusters have the same shape)
# The SHAPE of the distribution: per-segment counts, sorted -- not the
# per-segment mapping.  Two clusters built from the same binary with the same
# DDL, opclass and segment count still place a given key on different segments
# (verified: id=1 lands on seg0 of one cluster and on seg2 of the other).  The
# apply side routes with the TARGET table's own cdbhash, i.e. exactly where a
# local INSERT would put the row, so the mapping may differ; what must hold is
# that the rows are spread the same way.
# (sorted in the shell: string_agg(... ORDER BY ...) is not order-stable under
# the two-stage aggregation ORCA builds -- segment partials are concatenated in
# arrival order)
segs() { psql -p "$1" -d "$2" -Atq -c "SELECT count(*) FROM $3 GROUP BY gp_segment_id" | sort -n | tr '\n' ',' ; }

wait_match() { # table, timeout
  local s t
  for k in $(seq 1 $2); do
    s=$(ck $SRC_PORT $SRC_DB $1); t=$(ck $TGT_PORT $TGT_DB $1)
    [ "$s" = "$t" ] && { echo "  $1 in sync at +${k}s ($s)"; return 0; }
    sleep 1
  done
  echo "  $1 NOT in sync after $2 s: source=$s target=$t"
  # what exactly is missing, while the tables are still there
  $S -c "COPY (SELECT * FROM $1 ORDER BY 1) TO STDOUT" > /tmp/r12_src_$1.txt 2>/dev/null
  $T -c "COPY (SELECT * FROM $1 ORDER BY 1) TO STDOUT" > /tmp/r12_tgt_$1.txt 2>/dev/null
  comm -23 <(sort /tmp/r12_src_$1.txt) <(sort /tmp/r12_tgt_$1.txt) > /tmp/r12_only_src_$1.txt
  comm -13 <(sort /tmp/r12_src_$1.txt) <(sort /tmp/r12_tgt_$1.txt) > /tmp/r12_only_tgt_$1.txt
  echo "    on the source only ($(wc -l < /tmp/r12_only_src_$1.txt) rows):"
  head -12 /tmp/r12_only_src_$1.txt | sed 's/^/      /'
  echo "    on the target only ($(wc -l < /tmp/r12_only_tgt_$1.txt) rows):"
  head -12 /tmp/r12_only_tgt_$1.txt | sed 's/^/      /'
  # which source segment carried them, and which kind of transaction wrote them
  local ids
  ids=$(cut -f1 /tmp/r12_only_src_$1.txt | paste -sd, -)
  if [ -n "$ids" ]; then
    echo "    the missing rows, by source segment:"
    $S -c "SELECT '      seg '||seg||': '||n||' rows' FROM (SELECT gp_segment_id AS seg, count(*) AS n FROM $1 WHERE id IN ($ids) GROUP BY 1) x ORDER BY seg"
    echo "    ... and by what wrote them:"
    $S -c "SELECT '      '||tag||': '||n||' rows' FROM (SELECT left(v,4) AS tag, count(*) AS n FROM $1 WHERE id IN ($ids) GROUP BY 1) x ORDER BY tag" 2>/dev/null
  fi
  return 1
}

drop_slots() {
  for k in $(seq 1 20); do
    $S -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE '${SLOT}%'" >/dev/null 2>&1
    $S -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name LIKE '${SLOT}%'" >/dev/null 2>&1
    [ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name LIKE '${SLOT}%'")" = "0" ] && return 0
    sleep 0.5
  done
  echo "WARNING: ${SLOT}* could not be dropped everywhere"
}

cleanup() {
  $T -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
  $T -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  for k in $(seq 1 30); do
    n=$($T -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'logical replication worker' OR application_name IN ('mpp_apply_worker','mpp_apply_sync')")
    [ "$n" = "0" ] && break; sleep 0.5
  done
  drop_slots
  $S -c "DROP PUBLICATION IF EXISTS $PUB" >/dev/null 2>&1
  $S -c "DROP TABLE IF EXISTS r12_hash, r12_rand, r12_repl, r12_ao, r12_late CASCADE" >/dev/null 2>&1
  $T -c "DROP TABLE IF EXISTS r12_hash, r12_rand, r12_repl, r12_ao, r12_late CASCADE" >/dev/null 2>&1
}

ddl() { # run the same DDL on both clusters
  $S -c "$1" >/dev/null 2>&1; $T -c "$1" >/dev/null 2>&1
}

echo "==== source :$SRC_PORT -> target :$TGT_PORT"
[ "$($S -c 'SHOW wal_level')" = "logical" ] || { echo "source wal_level is not logical"; exit 2; }
SRC_SEGS=$($S -c "SELECT count(*) FROM gp_segment_configuration WHERE role='p' AND content>=0")
TGT_SEGS=$($T -c "SELECT count(*) FROM gp_segment_configuration WHERE role='p' AND content>=0")
echo "     source segments: $SRC_SEGS, target segments: $TGT_SEGS"
TGT_LOG_DIR=$(tgt_datadir)/log

echo "==== cleanup"; cleanup

# ------------------------------------------------------------------ schema
echo; echo "==== 1. schema + seed + subscription (initial sync under load)"
ddl "CREATE TABLE r12_hash (id int PRIMARY KEY, v text, n int) DISTRIBUTED BY (id)"
ddl "CREATE TABLE r12_rand (id int, v text) DISTRIBUTED RANDOMLY"
ddl "CREATE TABLE r12_repl (id int PRIMARY KEY, v text) DISTRIBUTED REPLICATED"
# r12_ao is heap on the source and AO on the target: that is the supported
# combination (the writer applies into AO/AOCO, R6/V7).  A source AO table
# cannot be replicated at all -- appendonly_decode() is an empty stub, so only
# its initial COPY ever arrives.
$S -c "CREATE TABLE r12_ao (id int, v text) DISTRIBUTED BY (id)" >/dev/null
$T -c "CREATE TABLE r12_ao (id int, v text) WITH (appendonly=true) DISTRIBUTED BY (id)" >/dev/null
ddl "CREATE TABLE r12_late (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$S -c "ALTER TABLE r12_rand REPLICA IDENTITY FULL" >/dev/null
$T -c "ALTER TABLE r12_rand REPLICA IDENTITY FULL" >/dev/null
$S -c "ALTER TABLE r12_ao REPLICA IDENTITY FULL" >/dev/null
$T -c "ALTER TABLE r12_ao REPLICA IDENTITY FULL" >/dev/null
$S -c "INSERT INTO r12_hash SELECT g, 'seed', g%7 FROM generate_series(1,20000) g" >/dev/null
$S -c "INSERT INTO r12_rand SELECT g, 'seed' FROM generate_series(1,3000) g" >/dev/null
$S -c "INSERT INTO r12_repl SELECT g, 'seed' FROM generate_series(1,500) g" >/dev/null
$S -c "INSERT INTO r12_ao SELECT g, 'seed' FROM generate_series(1,5000) g" >/dev/null
$S -c "INSERT INTO r12_late SELECT g, 'seed' FROM generate_series(1,4000) g" >/dev/null
$S -c "CREATE PUBLICATION $PUB FOR TABLE r12_hash, r12_rand, r12_repl, r12_ao" >/dev/null

# a workload that spans the whole run
workload() {
  local b=100 end=$((SECONDS+LOAD_SECS))
  while [ $SECONDS -lt $end ]; do
    b=$((b+1)); local base=$((b*1000))
    $S -c "INSERT INTO r12_hash SELECT g,'w$b',$b FROM generate_series($base,$((base+9))) g" >/dev/null 2>&1   # 2PC, many segments
    $S -c "INSERT INTO r12_hash VALUES ($((base+50)),'one',$b)" >/dev/null 2>&1                                # one-phase
    $S -c "UPDATE r12_hash SET v='u$b' WHERE id=$((base+3))" >/dev/null 2>&1
    $S -c "DELETE FROM r12_hash WHERE id=$((base+5))" >/dev/null 2>&1
    $S -c "UPDATE r12_hash SET v='nomatch' WHERE id=$((base+90000))" >/dev/null 2>&1
    $S -c "BEGIN; INSERT INTO r12_hash VALUES ($((base+60)),'sp',$b); SAVEPOINT s; INSERT INTO r12_hash VALUES ($((base+61)),'gone',$b); ROLLBACK TO s; UPDATE r12_hash SET v='multi' WHERE id=$((base+7)); COMMIT" >/dev/null 2>&1
    $S -c "INSERT INTO r12_rand VALUES ($base,'r$b')" >/dev/null 2>&1
    $S -c "UPDATE r12_rand SET v='ru$b' WHERE id=$((b*3))" >/dev/null 2>&1
    $S -c "INSERT INTO r12_repl VALUES ($base,'p$b')" >/dev/null 2>&1
    $S -c "INSERT INTO r12_ao SELECT g,'a$b' FROM generate_series($base,$((base+4))) g" >/dev/null 2>&1
    $S -c "INSERT INTO r12_late VALUES ($base,'l$b')" >/dev/null 2>&1
    [ $((b%7)) -eq 0 ] && $S -c "INSERT INTO r12_hash SELECT g,'big$b',$b FROM generate_series($((base+100000)),$((base+140000))) g" >/dev/null 2>&1  # big/streamed
    sleep 0.2
  done
}

workload & WL=$!
sleep 3
echo "-- CREATE SUBSCRIPTION while the workload runs"
$T -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$SRC_PORT dbname=$SRC_DB' PUBLICATION $PUB WITH (mpp=on, slot_name='$SLOT')" 2>&1 | grep -vE '^NOTICE|^WARNING|^$|^HINT' | head -3

# ------------------------------------------------------------------ faults
echo; echo "==== 2-3. faults while the workload runs"
sleep 8
rp=$($T -c "SELECT pid FROM gp_dist_random('pg_stat_activity') WHERE query LIKE '%LogicalSource%' OR query LIKE '%gp_lr_drain%' LIMIT 1")
$T -c "SELECT pg_terminate_backend(pid) FROM gp_dist_random('pg_stat_activity') WHERE (query LIKE '%gp_lr_%') AND gp_segment_id = 1" >/dev/null 2>&1
echo "  [t+8]  killed the reader QE on target segment 1"
sleep 6
$T -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'mpp_apply_worker' AND backend_type = 'client backend'" >/dev/null 2>&1
echo "  [t+14] killed the driven QD session"
sleep 6
# not pg_stat_subscription: the server's subscription is disabled and it
# knows of no worker.  The module's worker is a background worker of its own
# type, and this database has exactly one.
wpid=$($T -c "SELECT pid FROM pg_stat_activity WHERE backend_type = 'mpp apply worker'
                AND datname = current_database() LIMIT 1")
[ -n "$wpid" ] && kill -9 "$wpid" 2>/dev/null && echo "  [t+20] SIGKILLed the apply worker (pid $wpid)"
sleep 6
$S -c "SELECT pg_terminate_backend(pid) FROM gp_dist_random('pg_stat_activity') WHERE backend_type = 'walsender'" >/dev/null 2>&1
echo "  [t+26] killed the source segment walsenders"

# ------------------------------------------------------- add a table (T4)
echo; echo "==== 4. add r12_late to the publication of the running subscription"
$S -c "ALTER PUBLICATION $PUB ADD TABLE r12_late" >/dev/null
$T -c "ALTER SUBSCRIPTION $SUB REFRESH PUBLICATION" 2>&1 | grep -vE '^ALTER|^NOTICE|^$' | head -2

wait $WL; echo "-- workload finished"

# ------------------------------------------------------------------ verify
echo; echo "==== 5. verification"
for t in r12_hash r12_rand r12_repl r12_ao r12_late; do
  wait_match $t 120 || fail "$t did not converge"
done

if [ "$SRC_SEGS" = "$TGT_SEGS" ]; then
  for t in r12_hash r12_ao; do
    a=$(segs $SRC_PORT $SRC_DB $t); b=$(segs $TGT_PORT $TGT_DB $t)
    [ "$a" = "$b" ] && ok "$t distribution shape matches ($a)" || fail "$t distribution shape differs: source=$a target=$b"
  done
fi

echo "-- TRUNCATE replication"
$S -c "TRUNCATE r12_ao" >/dev/null
wait_match r12_ao 60 || fail "TRUNCATE did not replicate"

echo "-- state"
SUBID=$($T -c "SELECT oid FROM pg_subscription WHERE subname='$SUB'")
$T -c "SELECT relid::regclass::text || ' -> ' || state::text FROM pg_catalog.gp_subscription_rel WHERE subid = $SUBID AND relid <> 0 ORDER BY 1"
[ "$($T -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid = $SUBID AND state <> 'r'")" = "0" ] \
  && ok "every table is ready" || fail "some tables are not ready"
sleep 3
napplied=$($T -c "SELECT count(*) FROM pg_catalog.gp_subscription_applied")
[ "$napplied" -lt 50 ] && ok "applied set trimmed ($napplied rows)" || fail "applied set not trimmed ($napplied rows)"
$T -c "SELECT 'progress: coordinator ' || coordinator_lsn || '  seg_pos ' || seg_pos::text || '  seg_flush ' || seg_flush::text FROM pg_catalog.gp_subscription_progress"

echo "-- worker log (target coordinator)"
LOG=$(ls -t $TGT_LOG_DIR/*.csv | head -1)
grep -oE 'mpp apply worker[^"]{0,120}' "$LOG" | grep -E 'started|recover|caught up|added table|not ready|stopped' | tail -8
nrec=$(grep -c 'recovering after' "$LOG")
echo "  recoveries in this run: $nrec (expected >= 1, the injected faults)"
[ "$nrec" -ge 1 ] && ok "the driver recovered from the injected faults" || fail "no recovery was recorded"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
