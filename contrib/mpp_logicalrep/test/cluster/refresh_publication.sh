#!/bin/bash
# A table added to a running MPP subscription is copied without stopping the
# stream of the others, and goes live only once the stream has passed its cut
# point (the "gate" in mpp_tablesync.c).
#
#   1  base subscription on t4_a, synced and streaming
#   2  ALTER PUBLICATION ADD TABLE t4_b (1.5M rows, and rows written to it
#      before the refresh) + ALTER SUBSCRIPTION ... REFRESH PUBLICATION:
#      while t4_b is copied, t4_a keeps being applied; changes to t4_b made
#      during the copy are held and replayed afterwards
#   3  both tables end up identical to the source, t4_b without duplicates
#   4  REFRESH runs inside a transaction block; a table dropped from the
#      publication loses its state row
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

ck() { psql -p $PORT -d "$1" -Atq -c "SELECT count(*)||'/'||coalesce(sum(hashtext($3)::bigint),0) FROM $2"; }
wait_match() {
  for k in $(seq 1 $3); do
    s=$(ck postgres $1 "$2"); t=$(ck lrdst $1 "$2")
    [ "$s" = "$t" ] && { echo "  $1 caught up at +${k}s ($s)"; return 0; }
    sleep 1
  done
  echo "  $1 NOT caught up after $3 s: source=$s target=$t"; return 1
}
wlog() { grep -o "mpp apply worker: subscription [0-9]*: [^\"]*" "$LOG" | grep -E "$1"; }

cleanup() {
  $D -c "ALTER SUBSCRIPTION t4_sub DISABLE" >/dev/null 2>&1
  $D -c "DROP SUBSCRIPTION IF EXISTS t4_sub" >/dev/null 2>&1
  for k in $(seq 1 30); do
    n=$($D -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'logical replication worker' OR application_name IN ('mpp_apply_worker','mpp_apply_sync')")
    [ "$n" = "0" ] && break; sleep 0.5
  done
  left=1
  for k in $(seq 1 20); do
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE 't4_slot%'" >/dev/null 2>&1
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name LIKE 't4_slot%'" >/dev/null 2>&1
    left=$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name LIKE 't4_slot%'")
    [ "$left" = "0" ] && break; sleep 0.5
  done
  [ "$left" = "0" ] || echo "WARNING: $left t4_slot* slots could not be dropped"
  $Q -c "DROP PUBLICATION IF EXISTS t4_pub" >/dev/null 2>&1
  $Q -c "DROP TABLE IF EXISTS t4_a, t4_b, t4_c" >/dev/null
  $D -c "DROP TABLE IF EXISTS t4_a, t4_b, t4_c" >/dev/null
}

echo "==== cleanup"; cleanup
MARK=$(wc -l < "$LOG")

# ------------------------------------------------------------------ phase 1
echo; echo "==== 1. base subscription on t4_a"
$Q -c "CREATE TABLE t4_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO t4_a SELECT g, 'seed' FROM generate_series(1,2000) g"
$Q -c "CREATE PUBLICATION t4_pub FOR TABLE t4_a"
$D -c "CREATE TABLE t4_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$D -c "CREATE SUBSCRIPTION t4_sub CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION t4_pub WITH (slot_name='t4_slot')" 2>&1 | grep -vE '^NOTICE|^WARNING|^$' | head -3
wait_match t4_a "id||'|'||v" 60 || fail "phase 1 t4_a"

# ------------------------------------------------------------------ phase 2
echo; echo "==== 2. add t4_b (1.5M rows) to the publication and REFRESH"
$Q -c "CREATE TABLE t4_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id)"
$D -c "CREATE TABLE t4_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO t4_b SELECT g, 'seed', g FROM generate_series(1,1500000) g"
$Q -c "ALTER PUBLICATION t4_pub ADD TABLE t4_b"
# written after the publication carries t4_b but before the refresh: the
# target may apply these, and the COPY contains them too -> the truncate on
# redo is what keeps them from being duplicated
$Q -c "INSERT INTO t4_b SELECT g, 'pre', g FROM generate_series(2000001,2000050) g"
sleep 2

# a workload on both tables that runs across the whole copy
( b=5; end=$((SECONDS+40)); while [ $SECONDS -lt $end ]; do b=$((b+1)); base=$((b*1000))
  $Q -c "INSERT INTO t4_a SELECT g,'new$b' FROM generate_series($base,$((base+9))) g" >/dev/null 2>&1
  $Q -c "UPDATE t4_a SET v='upd$b' WHERE id=$((b*7))" >/dev/null 2>&1
  $Q -c "DELETE FROM t4_a WHERE id=$((b*11))" >/dev/null 2>&1
  $Q -c "INSERT INTO t4_b VALUES ($((3000000+b)),'during$b',$b)" >/dev/null 2>&1
  $Q -c "UPDATE t4_b SET v='updb$b' WHERE id=$((b*13))" >/dev/null 2>&1
  $Q -c "DELETE FROM t4_b WHERE id=$((b*17))" >/dev/null 2>&1
  sleep 0.2
done ) &
WL=$!

echo "-- REFRESH inside a transaction block (not allowed for stock subscriptions)"
$D -c "BEGIN; ALTER SUBSCRIPTION t4_sub REFRESH PUBLICATION; COMMIT" 2>&1 | grep -vE '^BEGIN|^COMMIT|^$' | head -3
$D -c "SELECT relid::regclass::text || ' -> ' || state::text FROM pg_catalog.gp_subscription_rel WHERE relid <> 0 ORDER BY 1"

# while the copy runs: t4_a must keep moving, t4_b must stay untouched
seen_d=0; a1=""; a2=""; b_during=""
for k in $(seq 1 60); do
  st=$($D -c "SELECT state FROM pg_catalog.gp_subscription_rel WHERE relid = 't4_b'::regclass")
  if [ "$st" = "d" ]; then
    if [ $seen_d -eq 0 ]; then
      seen_d=1
      a1=$($D -c "SELECT count(*) FROM t4_a")
      b_during=$($D -c "SELECT count(*) FROM t4_b WHERE v LIKE 'during%'")
    fi
    a2=$($D -c "SELECT count(*) FROM t4_a")
  fi
  [ "$st" = "r" ] && break
  sleep 0.5
done
echo "-- during the copy: t4_a target rows $a1 -> $a2, t4_b 'during' rows visible: ${b_during:-n/a}"
[ $seen_d -eq 1 ] || fail "never observed t4_b in state 'd' (copy too fast?)"
[ -n "$a2" ] && [ "$a2" -gt "${a1:-0}" ] || fail "t4_a did not advance while t4_b was being copied"
[ "${b_during:-0}" = "0" ] || fail "t4_b received changes while it was being copied"

wait $WL; echo "-- workload done"

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. both tables consistent"
wait_match t4_a "id||'|'||v" 60 || fail "phase 3 t4_a"
wait_match t4_b "id||'|'||v||'|'||w" 60 || fail "phase 3 t4_b"
$D -c "SELECT relid::regclass::text || ' -> ' || state::text FROM pg_catalog.gp_subscription_rel WHERE relid <> 0 ORDER BY 1"
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE state <> 'r'")" = "0" ] || fail "state rows not all 'r'"
echo "-- worker log:"; wlog "not ready|synchronising|added table|caught up" | tail -8

# ------------------------------------------------------------------ phase 4
echo; echo "==== 4. dropping a table from the publication removes its state row"
$Q -c "ALTER PUBLICATION t4_pub DROP TABLE t4_b"
$D -c "ALTER SUBSCRIPTION t4_sub REFRESH PUBLICATION" 2>&1 | grep -vE '^ALTER|^NOTICE|^$' | head -3
sleep 1
n=$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE relid = 't4_b'::regclass")
[ "$n" = "0" ] || fail "state row of t4_b not removed ($n)"
echo "-- rows left: $($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel")"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && echo "ALL PASS" || echo "$FAILS FAILURE(S)"
