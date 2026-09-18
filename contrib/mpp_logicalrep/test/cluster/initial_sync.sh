#!/bin/bash
# CREATE SUBSCRIPTION drives the initial sync
# itself (mpp_tablesync.c): slots -> barrier -> cut point -> segment-to-segment
# COPY (T1/T2 transport) -> streaming with the DS_t filter.
#
#   1  auto initial sync of a 2-table publication under a concurrent workload
#   2  DISABLE / ENABLE: the worker restarts without re-syncing
#   3  copy_data = false: pre-seeded table, no COPY at all
#   4  sync interrupted (pg_terminate_backend of the worker mid-sync) -> redone
#      from scratch on restart; DROP SUBSCRIPTION removes the state rows
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
LOG=$(ls -t "$(psql -p $PORT -d postgres -Atqc 'SHOW data_directory')"/log/*.csv | head -1)
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }

# order-independent checksum of (id, v[, w])
ck() { psql -p $PORT -d "$1" -Atq -c "SELECT count(*)||'/'||coalesce(sum(hashtext($3)::bigint),0) FROM $2"; }
wait_match() {  # db-independent: wait until source ck == target ck for table $1 cols $2, up to $3 s
  for k in $(seq 1 $3); do
    s=$(ck postgres $1 "$2"); t=$(ck lrdst $1 "$2")
    [ "$s" = "$t" ] && { echo "  $1 caught up at +${k}s ($s)"; return 0; }
    sleep 1
  done
  echo "  $1 NOT caught up after $3 s: source=$s target=$t"; return 1
}
# The module's workers are not in pg_stat_subscription: the server's own
# subscription rows stay disabled, so the server knows of no worker.  They
# are background workers of a type of their own.
workers()    { $D -c "SELECT count(*) FROM pg_stat_activity
                       WHERE backend_type = 'mpp apply worker' AND datname = current_database()"; }
worker_pid() { $D -c "SELECT pid FROM pg_stat_activity
                       WHERE backend_type = 'mpp apply worker' AND datname = current_database()"; }
# wait until no worker is left in the database, so that the next phase's
# worker is the only one and can be identified by its type alone
quiesce()    { for k in $(seq 1 60); do [ "$(workers)" = "0" ] && return 0; sleep 0.5; done; return 1; }
wlog() { grep -o "mpp apply worker: subscription [0-9]*: [^\"]*" "$LOG" | grep -E "$1" ; }

cleanup() {
  for sub in t3_sub t3_sub2 t3_sub3; do $D -c "ALTER SUBSCRIPTION $sub DISABLE" >/dev/null 2>&1; $D -c "DROP SUBSCRIPTION IF EXISTS $sub" >/dev/null 2>&1; done
  # the workers and their reader QEs must be gone before the slots can be dropped
  for k in $(seq 1 30); do
    n=$($D -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'logical replication worker' OR application_name IN ('mpp_apply_worker','mpp_apply_sync')")
    [ "$n" = "0" ] && break; sleep 0.5
  done
  for k in $(seq 1 20); do
    # per node, so a half-dropped slot (coordinator gone, segments still held by readers) is retryable
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 't3_slot%'" >/dev/null 2>&1
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name LIKE 't3_slot%'" >/dev/null 2>&1
    left=$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name LIKE 't3_slot%'")
    [ "$left" = "0" ] && break; sleep 0.5
  done
  [ "$left" = "0" ] || echo "WARNING: $left t3_slot* slots could not be dropped"
  for p in t3_pub t3_pub2 t3_pub3; do $Q -c "DROP PUBLICATION IF EXISTS $p" >/dev/null 2>&1; done
  $Q -c "DROP TABLE IF EXISTS t3_a, t3_b, t3_c, t3_big1, t3_big2" >/dev/null
  $D -c "DROP TABLE IF EXISTS t3_a, t3_b, t3_c, t3_big1, t3_big2" >/dev/null
}

echo "==== cleanup"; cleanup

# ------------------------------------------------------------------ phase 1
echo; echo "==== 1. auto initial sync under load"
$Q -c "CREATE TABLE t3_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "CREATE TABLE t3_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO t3_a SELECT g, 'seed' FROM generate_series(1,2000) g"
$Q -c "INSERT INTO t3_b SELECT g, 'seed', g*2 FROM generate_series(1,1000) g"
$Q -c "CREATE PUBLICATION t3_pub FOR TABLE t3_a, t3_b"
$D -c "CREATE TABLE t3_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$D -c "CREATE TABLE t3_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id)"

( b=5; end=$((SECONDS+25)); while [ $SECONDS -lt $end ]; do b=$((b+1)); base=$((b*1000))
  $Q -c "INSERT INTO t3_a SELECT g,'new$b' FROM generate_series($base,$((base+9))) g" >/dev/null 2>&1
  $Q -c "UPDATE t3_a SET v='upd$b' WHERE id=$((b*7))" >/dev/null 2>&1
  $Q -c "DELETE FROM t3_a WHERE id=$((b*11))" >/dev/null 2>&1
  $Q -c "INSERT INTO t3_b VALUES ($base,'new$b',$b)" >/dev/null 2>&1
  $Q -c "UPDATE t3_b SET w=w+1 WHERE id=$((b*3))" >/dev/null 2>&1
done ) &
WL=$!
sleep 3
echo "-- CREATE SUBSCRIPTION (enabled, copy_data default) while the workload runs"
$D -c "CREATE SUBSCRIPTION t3_sub CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION t3_pub WITH (slot_name='t3_slot')" 2>&1 | grep -v '^NOTICE\|^WARNING\|^$' | head -3
wait $WL; echo "-- workload done"
wait_match t3_a "id||'|'||v" 60 || fail "phase 1 t3_a"
wait_match t3_b "id||'|'||v||'|'||w" 30 || fail "phase 1 t3_b"
SUB1=$($D -c "SELECT oid FROM pg_subscription WHERE subname='t3_sub'")
echo "-- state rows (relid 0 = marker):"; $D -c "SELECT relid, state, sync_lsn, left(sync_snapshot,40), copy_job FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB1 ORDER BY relid"
echo "-- slots on the source (expect 4: coordinator + 3 segments):"; $Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='t3_slot'"
# scoped to this subscription: other tests leave their own rows behind
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB1 AND state='r'")" = "3" ] || fail "phase 1 state rows not all 'r'"
[ "$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='t3_slot'")" = "4" ] || fail "phase 1 slots"
echo "-- worker log:"; wlog "initial synchronisation|barrier|cut point|copied|creating" | tail -8

# ------------------------------------------------------------------ phase 2
echo; echo "==== 2. DISABLE / ENABLE: no re-sync"
N1=$(wlog "initial synchronisation" | wc -l)
$D -c "ALTER SUBSCRIPTION t3_sub DISABLE"; sleep 2
echo "-- workers after disable: $($D -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='logical replication worker'")"
$Q -c "INSERT INTO t3_a SELECT g,'p2' FROM generate_series(90000,90099) g" >/dev/null
$Q -c "DELETE FROM t3_b WHERE id BETWEEN 10 AND 20" >/dev/null
$D -c "ALTER SUBSCRIPTION t3_sub ENABLE"
wait_match t3_a "id||'|'||v" 40 || fail "phase 2 t3_a"
wait_match t3_b "id||'|'||v||'|'||w" 20 || fail "phase 2 t3_b"
N2=$(wlog "initial synchronisation" | wc -l)
echo "-- 'initial synchronisation' log lines before/after: $N1 / $N2"
[ "$N1" = "$N2" ] || fail "phase 2 re-synced on restart"

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. copy_data = false: pre-seeded, no COPY"
$Q -c "CREATE TABLE t3_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO t3_c SELECT g, 'seed' FROM generate_series(1,300) g"
$Q -c "CREATE PUBLICATION t3_pub2 FOR TABLE t3_c"
$D -c "CREATE TABLE t3_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "COPY t3_c TO STDOUT" | $D -c "COPY t3_c FROM STDIN"
echo "-- pre-created slots (user's job with copy_data=false): $($Q -c "SELECT count(*) FROM gp_create_logical_replication_slot('t3_slot2','pgoutput',false,true)")"
$D -c "CREATE SUBSCRIPTION t3_sub2 CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION t3_pub2 WITH (slot_name='t3_slot2', copy_data=false)" 2>&1 | grep -v '^NOTICE\|^WARNING\|^$' | head -3
SUB2=$($D -c "SELECT oid FROM pg_subscription WHERE subname='t3_sub2'")
echo "-- marker written by CREATE: $($D -c "SELECT relid||':'||state::text FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB2")"
sleep 2
$Q -c "INSERT INTO t3_c SELECT g,'p3' FROM generate_series(1001,1050) g" >/dev/null
$Q -c "UPDATE t3_c SET v='u3' WHERE id=5" >/dev/null
wait_match t3_c "id||'|'||v" 40 || fail "phase 3 t3_c"
[ "$(wlog "subscription $SUB2: .*initial synchronisation" | wc -l)" = "0" ] || fail "phase 3 copied despite copy_data=false"
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB2")" = "1" ] || fail "phase 3 unexpected rel rows"

# ------------------------------------------------------------------ phase 4
echo; echo "==== 4. interrupted sync is redone; DROP cleans state"
# leave exactly one worker running, so the pid terminated below is certainly
# the one synchronising this phase's tables
for sub in t3_sub t3_sub2; do $D -c "ALTER SUBSCRIPTION $sub DISABLE" >/dev/null 2>&1; done
quiesce || fail "phase 4 could not quiesce the earlier subscriptions"
$Q -c "CREATE TABLE t3_big1 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "CREATE TABLE t3_big2 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO t3_big1 SELECT g, md5(g::text) FROM generate_series(1,1500000) g"
$Q -c "INSERT INTO t3_big2 SELECT g, md5(g::text) FROM generate_series(1,1500000) g"
$Q -c "CREATE PUBLICATION t3_pub3 FOR TABLE t3_big1, t3_big2"
$D -c "CREATE TABLE t3_big1 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$D -c "CREATE TABLE t3_big2 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$D -c "CREATE SUBSCRIPTION t3_sub3 CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION t3_pub3 WITH (slot_name='t3_slot3')" 2>&1 | grep -v '^NOTICE\|^WARNING\|^$' | head -3
SUB3=$($D -c "SELECT oid FROM pg_subscription WHERE subname='t3_sub3'")
# wait until the first table is being copied, then terminate the worker
for k in $(seq 1 100); do
  st=$($D -c "SELECT string_agg(relid||':'||state::text, ' ' ORDER BY relid) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB3")
  case "$st" in *":d "*|*":d") break;; esac
  sleep 0.2
done
echo "-- state when terminating: $st"
P=$(worker_pid); echo "-- terminating worker pid $P"
[ -n "$P" ] && $D -c "SELECT pg_terminate_backend($P)" >/dev/null || fail "phase 4 found no worker to terminate"
sleep 1
echo "-- state right after: $($D -c "SELECT string_agg(relid||':'||state::text, ' ' ORDER BY relid) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB3")"
$Q -c "INSERT INTO t3_big1 VALUES (2000001,'after')" >/dev/null
wait_match t3_big1 "id||'|'||v" 120 || fail "phase 4 t3_big1"
wait_match t3_big2 "id||'|'||v" 30 || fail "phase 4 t3_big2"
echo "-- worker log for sub $SUB3:"; wlog "subscription $SUB3:" | grep -E "initial synchronisation|interrupted|copied|redoing" | tail -8
[ "$(wlog "subscription $SUB3: redoing" | wc -l)" -ge 1 ] || fail "phase 4 no redo logged"
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB3 AND state='r'")" = "3" ] || fail "phase 4 state rows"
echo "-- DROP SUBSCRIPTION t3_sub3"
$D -c "ALTER SUBSCRIPTION t3_sub3 DISABLE"; sleep 1; $D -c "DROP SUBSCRIPTION t3_sub3"
echo "-- state rows left for sub $SUB3: rel=$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB3") progress=$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_progress WHERE subid=$SUB3") applied=$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_applied WHERE subid=$SUB3")"
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid=$SUB3")" = "0" ] || fail "phase 4 DROP left rel rows"
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_progress WHERE subid=$SUB3")" = "0" ] || fail "phase 4 DROP left progress rows"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS = 0 ] && echo "ALL PASS" || echo "FAILURES: $FAILS"
exit $FAILS
