#!/bin/bash
# The subscription DDL and the launcher, source and target in one cluster (two
# databases).  A loaded module means CREATE SUBSCRIPTION is taken apart by its
# ProcessUtility hook: the server's own subscription stays disabled, so its
# launcher starts nothing, and the state the module acts on is its own row in
# gp_mpp_subscription.
#
#   1  CREATE inside a transaction block is allowed (nothing connects to the
#      publisher), and lands in both catalogs
#   2  the module's launcher starts exactly one worker, on the coordinator, and
#      none on the segments (the server's launcher must not start a stock one)
#   3  changes replicate
#   4  DISABLE stops the worker and ENABLE brings it back
#   5  the options the module owns are refused with a reason: connect (in
#      either direction) and create_slot = true
#   6  enabled = false creates the subscription without starting it
#   7  there is no mpp option: every subscription here is the module's
#
# Usage: subscription_ddl.sh [-s <port>]   (default 7100, target database lrdst)
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null
PORT=7100
while getopts "s:" opt; do
  case $opt in
    s) PORT=$OPTARG ;;
    *) echo "usage: $0 [-s <port>]   (one cluster: database postgres -> lrdst)"; exit 2 ;;
  esac
done
Q="psql -p $PORT -d postgres -Atq"; D="psql -p $PORT -d lrdst -Atq"
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }
workers() { $D -c "SELECT count(*) FROM pg_stat_activity
                    WHERE backend_type = 'mpp apply worker' AND datname = current_database()"; }
# One worker per enabled row of the registry, once things settle.  Subscription
# DDL invalidates the subscription syscache for every worker in the database,
# so each of them stops and is started again a few seconds later: the count
# flaps while that happens, and only the settled value means anything.
settled_workers() {
  local want n k
  want=$($D -c "SELECT count(*) FROM pg_catalog.gp_mpp_subscription WHERE enabled")
  for k in $(seq 1 40); do
    n=$(workers)
    [ "$n" = "$want" ] && { echo "$n"; return 0; }
    sleep 0.5
  done
  echo "$n"; return 1
}
echo "==== source: table + publication + seed"
$Q -c "DROP PUBLICATION IF EXISTS r1_pub" >/dev/null 2>&1
$Q -c "DROP TABLE IF EXISTS r1_t" >/dev/null
$Q -c "CREATE TABLE r1_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO r1_t SELECT g, 'seed' FROM generate_series(1,50) g"
$Q -c "CREATE PUBLICATION r1_pub FOR TABLE r1_t"
echo "==== cluster-wide slots (coordinator + every segment, two_phase)"
$Q -c "SELECT * FROM gp_drop_replication_slot('r1_slot')" >/dev/null 2>&1
$Q -c "SELECT gp_segment_id FROM gp_create_logical_replication_slot('r1_slot','pgoutput',false,true) ORDER BY 1" | tr '\n' ' '; echo
echo "==== target db + table seeded from source"
$Q -c "SELECT count(*) FROM pg_database WHERE datname='lrdst'" | grep -q 1 || $Q -c "CREATE DATABASE lrdst" >/dev/null
$D -c "DROP TABLE IF EXISTS r1_t" >/dev/null; $D -c "CREATE TABLE r1_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "COPY r1_t TO STDOUT" | $D -c "COPY r1_t FROM STDIN"
echo "source=$($Q -c 'SELECT count(*) FROM r1_t') target=$($D -c 'SELECT count(*) FROM r1_t')"
echo "==== CREATE SUBSCRIPTION ... in a transaction block (allowed: nothing connects)"
for sub in r1_sub r1_off r1_stock; do
  $D -c "ALTER SUBSCRIPTION $sub SET (slot_name = NONE)" >/dev/null 2>&1
  $D -c "DROP SUBSCRIPTION IF EXISTS $sub" >/dev/null 2>&1
done
$D -c "BEGIN; CREATE SUBSCRIPTION r1_sub CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r1_pub WITH (slot_name='r1_slot', copy_data=false, two_phase=on); COMMIT" 2>&1 | grep -vE '^NOTICE|^DETAIL|^HINT|^$' | tail -1
[ "$($D -c "SELECT subenabled FROM pg_subscription WHERE subname='r1_sub'")" = "f" ] \
  && ok "the server's own subscription row stays disabled" \
  || fail "pg_subscription.subenabled is true: the server's launcher will start a worker of its own"
[ "$($D -c "SELECT enabled FROM pg_catalog.gp_mpp_subscription WHERE subname='r1_sub'")" = "t" ] \
  && ok "the module's registry has it, enabled" \
  || fail "no enabled row in gp_mpp_subscription"
echo "==== the module's launcher starts ONE worker on the coordinator, none on segments"
for k in $(seq 1 20); do [ "$(workers)" = "1" ] && break; sleep 0.5; done
[ "$(workers)" = "1" ] && ok "one worker on the coordinator" || fail "workers on the coordinator: $(workers), expected 1"
n=$($D -c "SELECT count(*) FROM gp_dist_random('pg_stat_activity')
            WHERE backend_type LIKE '%logical replication%worker%' OR backend_type = 'mpp apply worker'")
[ "$n" = "0" ] && ok "no apply worker on any segment" || fail "$n apply worker(s) on the segments"
echo "==== write to source, expect replication to target"
$Q -c "INSERT INTO r1_t SELECT g, 'r1' FROM generate_series(101,130) g" >/dev/null
$Q -c "INSERT INTO r1_t VALUES (201,'one')" >/dev/null
$Q -c "UPDATE r1_t SET v='upd' WHERE id=105" >/dev/null
$Q -c "DELETE FROM r1_t WHERE id=110" >/dev/null
md5() { $1 -c "COPY (SELECT id,v FROM r1_t ORDER BY id) TO STDOUT" | md5sum | cut -c1-12; }
for k in $(seq 1 30); do [ "$(md5 "$Q")" = "$(md5 "$D")" ] && break; sleep 1; done
[ "$(md5 "$Q")" = "$(md5 "$D")" ] \
  && ok "target matches the source at +${k}s ($(md5 "$Q"))" \
  || fail "target differs: source $(md5 "$Q") target $(md5 "$D")"
echo "==== DISABLE stops the worker, ENABLE brings it back"
$D -c "ALTER SUBSCRIPTION r1_sub DISABLE" >/dev/null
for k in $(seq 1 20); do [ "$(workers)" = "0" ] && break; sleep 0.5; done
[ "$(workers)" = "0" ] && ok "DISABLE stopped the worker" || fail "worker still running after DISABLE"
$D -c "ALTER SUBSCRIPTION r1_sub ENABLE" >/dev/null
for k in $(seq 1 20); do [ "$(workers)" = "1" ] && break; sleep 0.5; done
[ "$(workers)" = "1" ] && ok "ENABLE started it again" || fail "no worker after ENABLE"

echo; echo "==== options the module owns are refused, with a reason"
for opt in "connect=false" "connect=true" "create_slot=true"; do
  msg=$($D -c "CREATE SUBSCRIPTION r1_bad CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r1_pub WITH (slot_name='r1_slot', $opt)" 2>&1 | grep '^ERROR' | head -1)
  case "$msg" in
    *"not accepted"*|*"not supported"*) ok "$opt refused: ${msg#ERROR:  }" ;;
    "")                                 fail "$opt was accepted" ;;
    *)                                  fail "$opt refused for the wrong reason: $msg" ;;
  esac
done
msg=$($D -c "CREATE SUBSCRIPTION r1_bad CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r1_pub" 2>&1 | grep '^ERROR' | head -1)
case "$msg" in
  *"requires slot_name"*) ok "a missing slot_name is refused" ;;
  *)                      fail "a missing slot_name gave: ${msg:-no error at all}" ;;
esac

echo; echo "==== enabled = false creates the subscription without starting it"
$D -c "DROP SUBSCRIPTION IF EXISTS r1_off" >/dev/null 2>&1
$D -c "CREATE SUBSCRIPTION r1_off CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r1_pub WITH (slot_name='r1_slot', copy_data=false, enabled=false)" 2>&1 | grep -vE '^NOTICE|^DETAIL|^HINT|^$' | head -2
[ "$($D -c "SELECT enabled FROM pg_catalog.gp_mpp_subscription WHERE subname='r1_off'")" = "f" ] \
  && ok "registry row created disabled" || fail "enabled = false was ignored"
n=$(settled_workers) \
  && ok "still one worker, for the enabled subscription only" \
  || fail "workers settled at $n, expected one per enabled registry row"
$D -c "DROP SUBSCRIPTION r1_off" >/dev/null 2>&1

echo; echo "==== there is no mpp option: a loaded module applies every subscription"
$D -c "DROP SUBSCRIPTION IF EXISTS r1_stock" >/dev/null 2>&1
msg=$($D -c "CREATE SUBSCRIPTION r1_stock CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION r1_pub WITH (mpp=false, slot_name='r1_stock_slot')" 2>&1 | grep '^ERROR' | head -1)
case "$msg" in
  *'unrecognized subscription parameter'*mpp*) ok "\"mpp\" is not a subscription parameter" ;;
  "")  fail "mpp = false was accepted" ;;
  *)   fail "mpp = false gave: $msg" ;;
esac
[ "$($D -c "SELECT count(*) FROM pg_subscription WHERE subname='r1_stock'")" = "0" ] \
  && ok "and nothing was created" || fail "a subscription was left behind"

echo; echo "==== worker log"
f=$(ls -t "$($Q -c 'SHOW data_directory')"/log/*.csv | head -1)
grep -o "mpp apply worker: [^\"]*" $f | grep -v "batch .* committed" | tail -5

echo; echo "==== cleanup"
$D -c "ALTER SUBSCRIPTION r1_sub DISABLE" >/dev/null 2>&1
$D -c "DROP SUBSCRIPTION IF EXISTS r1_sub" >/dev/null 2>&1
$Q -c "SELECT * FROM gp_drop_replication_slot('r1_slot')" >/dev/null 2>&1
$Q -c "DROP PUBLICATION IF EXISTS r1_pub" >/dev/null 2>&1
$Q -c "DROP TABLE IF EXISTS r1_t" >/dev/null 2>&1; $D -c "DROP TABLE IF EXISTS r1_t" >/dev/null 2>&1

echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
