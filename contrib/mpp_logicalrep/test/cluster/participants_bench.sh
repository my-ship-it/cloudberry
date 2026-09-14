#!/bin/bash
# Grace-period cost: how long a stream of small distributed transactions takes
# to reach the target.  A single-key UPDATE on a hash table is dispatched to
# every segment as 2PC but only one segment writes, so without the precise
# participant list in the commit record the driver would wait
# mpp_logicalrep.event_wait_ms for the silent segments every time.  Single-key
# INSERTs are one-phase, where the participant is exact by construction, and
# serve as the reference.
#   usage: participants_bench.sh [-s <port>] [-n <statements per kind>]
set -u
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null
PORT=7100; N=100
while getopts "s:n:" opt; do
  case $opt in
    s) PORT=$OPTARG ;; n) N=$OPTARG ;;
    *) echo "usage: $0 [-s <port>] [-n <statements per kind>]"; exit 2 ;;
  esac
done
Q="psql -p $PORT -d postgres -Atq -v ON_ERROR_STOP=0"
D="psql -p $PORT -d lrdst -Atq -v ON_ERROR_STOP=0"
# resolved when it is read: the coordinator may rotate its log meanwhile
logs() { ls -t "$(psql -p $PORT -d postgres -Atqc 'SHOW data_directory')"/log/*.csv | head -2; }
elapsed() { awk -v a="$1" -v b="$(date +%s%N)" 'BEGIN{printf "%.1f", (b-a)/1e9}'; }
ck() { psql -p $PORT -d "$1" -Atq -c "SELECT count(*)||'/'||coalesce(sum(hashtext(id||'|'||v)::bigint),0) FROM r8p"; }
wait_match() {
  local t0=$(date +%s%N)
  for k in $(seq 1 600); do
    [ "$(ck postgres)" = "$(ck lrdst)" ] && { echo "  caught up in $(elapsed $t0) s"; return 0; }
    sleep 0.2
  done
  echo "  NOT caught up"; return 1
}
cleanup() {
  $D -c "ALTER SUBSCRIPTION r8p_sub DISABLE" >/dev/null 2>&1; $D -c "DROP SUBSCRIPTION IF EXISTS r8p_sub" >/dev/null 2>&1
  for k in $(seq 1 30); do n=$($D -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='logical replication worker' OR application_name IN ('mpp_apply_worker','mpp_apply_sync')"); [ "$n" = "0" ] && break; sleep 0.5; done
  for k in $(seq 1 20); do
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='r8p_slot'" >/dev/null 2>&1
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name='r8p_slot'" >/dev/null 2>&1
    [ "$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='r8p_slot'")" = "0" ] && break; sleep 0.5
  done
  $Q -c "DROP PUBLICATION IF EXISTS r8p_pub" >/dev/null 2>&1
  $Q -c "DROP TABLE IF EXISTS r8p" >/dev/null; $D -c "DROP TABLE IF EXISTS r8p" >/dev/null
}

echo "==== setup"; cleanup
$Q -c "CREATE TABLE r8p (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO r8p SELECT g, 'seed' FROM generate_series(1,1000) g"
$Q -c "CREATE PUBLICATION r8p_pub FOR TABLE r8p"
$D -c "CREATE TABLE r8p (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "COPY r8p TO STDOUT" | $D -c "COPY r8p FROM STDIN"
$Q -c "SELECT count(*) FROM gp_create_logical_replication_slot('r8p_slot','pgoutput',false,true)" >/dev/null
$D -c "CREATE SUBSCRIPTION r8p_sub CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r8p_pub WITH (mpp=on, slot_name='r8p_slot', copy_data=false)" 2>&1 | grep -E '^ERROR'
sleep 3
$Q -c "UPDATE r8p SET v='warm' WHERE id=1" >/dev/null; wait_match >/dev/null

echo "==== $N single-key UPDATEs (2PC dispatched to all segments, one writer)"
t0=$(date +%s%N)
for i in $(seq 1 $N); do $Q -c "UPDATE r8p SET v='u$i' WHERE id=$((i+1))" >/dev/null; done
echo "  issued in $(elapsed $t0) s"
wait_match

echo "==== $N single-key INSERTs (one-phase, precise participant)"
t0=$(date +%s%N)
for i in $(seq 1 $N); do $Q -c "INSERT INTO r8p VALUES ($((100000+i)), 'i$i')" >/dev/null; done
echo "  issued in $(elapsed $t0) s"
wait_match

echo "==== $N single-key DELETEs"
t0=$(date +%s%N)
for i in $(seq 1 $N); do $Q -c "DELETE FROM r8p WHERE id=$((500+i))" >/dev/null; done
echo "  issued in $(elapsed $t0) s"
wait_match

echo "==== worker stats"
SUBID=$($D -c "SELECT oid FROM pg_subscription WHERE subname='r8p_sub'")
$D -c "ALTER SUBSCRIPTION r8p_sub DISABLE"
for k in $(seq 1 20); do
  grep -qho "subscription $SUBID stopped:" $(logs) && break; sleep 0.5
done
grep -ho "mpp apply worker: subscription $SUBID stopped: [^\"]*" $(logs) | tail -1
echo "==== cleanup"; cleanup
