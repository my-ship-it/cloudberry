#!/bin/bash
# Fault injection under mixed load, source and target in one cluster (two
# databases).  Kill a reader QE, the driven QD session,
# and the worker bgworker; after the workload stops the worker must catch up and
# source == target (no loss, no duplication).
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null
PORT=7100
while getopts "s:" opt; do
  case $opt in
    s) PORT=$OPTARG ;;
    *) echo "usage: $0 [-s <port>]   (one cluster: database postgres -> lrdst)"; exit 2 ;;
  esac
done
Q="psql -p $PORT -d postgres -Atq"; D="psql -p $PORT -d lrdst -Atq"; SUB=r9_sub SLOT=r9_slot
d(){ psql -p $PORT -d "$1" -Atq -c "SELECT count(*) FROM r9_t"|tr -d '\n'; echo -n /; psql -p $PORT -d "$1" -Atq -c "COPY (SELECT id,v FROM r9_t ORDER BY id) TO STDOUT"|md5sum|cut -c1-12; }

echo "==== setup"
$D -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
# per node: gp_drop_replication_slot() is not idempotent (a half-dropped slot makes it fail forever)
$Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='$SLOT'" >/dev/null 2>&1
$Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name='$SLOT'" >/dev/null 2>&1
$Q -c "DROP PUBLICATION IF EXISTS r9_pub" >/dev/null 2>&1; $Q -c "DROP TABLE IF EXISTS r9_t" >/dev/null
$Q -c "CREATE TABLE r9_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO r9_t SELECT g,'seed' FROM generate_series(1,100) g"
$Q -c "CREATE PUBLICATION r9_pub FOR TABLE r9_t"
$Q -c "SELECT gp_segment_id FROM gp_create_logical_replication_slot('$SLOT','pgoutput',false,true)" >/dev/null
$D -c "DROP TABLE IF EXISTS r9_t" >/dev/null; $D -c "CREATE TABLE r9_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "COPY r9_t TO STDOUT" | $D -c "COPY r9_t FROM STDIN"
$D -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r9_pub WITH (mpp, slot_name='$SLOT', copy_data=false, two_phase=on)" 2>&1 | tail -1
$D -c "ALTER SUBSCRIPTION $SUB ENABLE"; sleep 3
echo "seed source=$(d postgres) target=$(d lrdst)"

echo "==== continuous mixed workload in background (~40s)"
( b=1000; end=$((SECONDS+40)); while [ $SECONDS -lt $end ]; do b=$((b+1)); base=$((b*100))
  $Q -c "INSERT INTO r9_t SELECT g,'w$b' FROM generate_series($base,$((base+9))) g" >/dev/null 2>&1
  $Q -c "INSERT INTO r9_t VALUES ($((base+50)),'one')" >/dev/null 2>&1
  $Q -c "UPDATE r9_t SET v='u$b' WHERE id=$((base+3))" >/dev/null 2>&1
  $Q -c "DELETE FROM r9_t WHERE id=$((base+5))" >/dev/null 2>&1
  $Q -c "BEGIN; INSERT INTO r9_t VALUES ($((base+60)),'sp'); SAVEPOINT s; INSERT INTO r9_t VALUES ($((base+61)),'x'); ROLLBACK TO s; UPDATE r9_t SET v='m' WHERE id=$((base+7)); COMMIT" >/dev/null 2>&1
done ) &
WL=$!

kill_reader(){ $D -c "SELECT pg_terminate_backend(pid) FROM gp_dist_random('pg_stat_activity') WHERE content=1 AND application_name LIKE 'mpp_apply%'" >/dev/null 2>&1; echo "  [t+$1] killed a reader QE on seg1"; }
kill_qd(){ local p=$($D -Atc "SELECT pid FROM pg_stat_activity WHERE application_name='mpp_apply_worker' AND backend_type='client backend' LIMIT 1"); [ -n "$p" ] && { $D -c "SELECT pg_terminate_backend($p)" >/dev/null 2>&1; echo "  [t+$1] killed the driven QD session (pid $p)"; } || echo "  [t+$1] no driven QD session found"; }
kill_worker(){ local p=$($D -Atc "SELECT pid FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database() LIMIT 1"); [ -n "$p" ] && { kill -9 $p 2>/dev/null; echo "  [t+$1] kill -9 worker bgworker (pid $p)"; } || echo "  [t+$1] no worker found"; }

sleep 8;  kill_reader 8
sleep 8;  kill_qd 16
sleep 8;  kill_worker 24
sleep 8;  kill_reader 32

wait $WL; echo "workload done, source=$(d postgres)"
echo "==== let the worker catch up (up to 60s)"
for k in $(seq 1 60); do [ "$(d postgres)" = "$(d lrdst)" ] && { echo "CAUGHT UP at +${k}s"; break; }; sleep 1; done
echo "source=$(d postgres)"; echo "target=$(d lrdst)"
[ "$(d postgres)" = "$(d lrdst)" ] && echo "RESULT=MATCH" || echo "RESULT=MISMATCH"
echo "recoveries + batches (worker log):"
f=$(ls -t "$(psql -p $PORT -d postgres -Atqc 'SHOW data_directory')"/log/*.csv | head -1)
echo "  recoveries: $(grep -c 'mpp apply worker: recovering' $f)  batches: $(grep -c 'mpp apply worker: batch .* committed' $f)"
$D -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
