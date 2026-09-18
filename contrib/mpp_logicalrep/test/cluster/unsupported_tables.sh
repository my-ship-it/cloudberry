#!/bin/bash
# A published table whose changes the publisher cannot decode.  Only heap
# tables produce logical decoding output: append-optimised tables have no
# decoding routine at all, so an AO table in a publication used to be copied
# once by the initial synchronisation and then silently stop being true.
#
#   1  the option itself: a bad value, and a value on a subscription that is
#      not ours
#   2  unsupported_tables = 'error' (the default): the subscription does not
#      start, disables itself at once and names the table and its access
#      method -- rather than restarting for ever
#   3  unsupported_tables = 'snapshot': the table is copied once and marked
#      's' in gp_subscription_rel; the heap tables of the same subscription
#      keep streaming while the AO table stays as it was copied
#   4  ALTER SUBSCRIPTION ... REFRESH PUBLICATION is refused on the spot in
#      'error' mode, and warns and copies in 'snapshot' mode
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
SLOT=a1_slot; PUB=a1_pub; SUB=a1_sub
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }

reg()   { $D -c "SELECT $1 FROM pg_catalog.gp_mpp_subscription WHERE subname='$SUB'"; }
subid() { $D -c "SELECT oid FROM pg_subscription WHERE subname='$SUB'"; }
relstate() { $D -c "SELECT state FROM pg_catalog.gp_subscription_rel
                     WHERE subid=$(subid) AND relid='$1'::regclass"; }
states() { $D -c "SELECT CASE WHEN relid=0 THEN '(marker)' ELSE relid::regclass::text END||'='||state::text
                    FROM pg_catalog.gp_subscription_rel WHERE subid=$(subid) ORDER BY 1" | tr '\n' ' '; }
# wait until <sql> on lrdst returns <want>, up to <n> seconds
waitfor() {
  local want=$1 n=$2 sql=$3 got=
  for k in $(seq 1 "$n"); do
    got=$($D -c "$sql")
    [ "$got" = "$want" ] && { echo "  ($sql) = $want at +${k}s"; return 0; }
    sleep 1
  done
  echo "  ($sql) = $got after $n s, wanted $want"; return 1
}

cleanup() {
  $D -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
  $D -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  for k in $(seq 1 30); do
    n=$($D -c "SELECT count(*) FROM pg_stat_activity
                WHERE backend_type='mpp apply worker'
                   OR application_name IN ('mpp_apply_worker','mpp_apply_sync')")
    [ "$n" = "0" ] && break; sleep 0.5
  done
  for k in $(seq 1 20); do
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='$SLOT'" >/dev/null 2>&1
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name='$SLOT'" >/dev/null 2>&1
    left=$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")
    [ "$left" = "0" ] && break; sleep 0.5
  done
  $Q -c "DROP PUBLICATION IF EXISTS $PUB" >/dev/null 2>&1
  $Q -c "DROP TABLE IF EXISTS a1_h, a1_ao, a1_ao2" >/dev/null 2>&1
  $D -c "DROP TABLE IF EXISTS a1_h, a1_ao, a1_ao2" >/dev/null 2>&1
}

echo "==== cluster :$PORT, postgres -> lrdst"
echo "==== cleanup"; cleanup

echo "-- source: one heap table, one ao_row table, both in the publication"
$Q -c "CREATE TABLE a1_h  (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "CREATE TABLE a1_ao (id int PRIMARY KEY, v text) USING ao_row DISTRIBUTED BY (id)"
$Q -c "INSERT INTO a1_h  SELECT g,'seed' FROM generate_series(1,100) g"
$Q -c "INSERT INTO a1_ao SELECT g,'seed' FROM generate_series(1,100) g"
$Q -c "CREATE PUBLICATION $PUB FOR TABLE a1_h, a1_ao"
$D -c "CREATE TABLE a1_h  (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$D -c "CREATE TABLE a1_ao (id int PRIMARY KEY, v text) USING ao_row DISTRIBUTED BY (id)"
$Q -c "SELECT count(*) FROM gp_create_logical_replication_slot('$SLOT','pgoutput',false,true)" >/dev/null

# ------------------------------------------------------------------ phase 1
echo; echo "==== 1. the option"
CONN="CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION $PUB"
out=$($D -c "CREATE SUBSCRIPTION a1_nope $CONN WITH (slot_name='a1_nope', unsupported_tables='bogus')" 2>&1)
echo "$out" | grep -q 'must be "error" or "snapshot"' \
  && ok "a bad value is refused" || fail "bad value accepted: $out"
out=$($D -c "CREATE SUBSCRIPTION a1_nope $CONN WITH (mpp=false, slot_name='a1_nope', unsupported_tables='snapshot')" 2>&1)
echo "$out" | grep -q 'only accepted for a subscription applied by mpp_logicalrep' \
  && ok "refused on a subscription that is not ours" || fail "accepted with mpp=false: $out"

# ------------------------------------------------------------------ phase 2
echo; echo "==== 2. the default: the subscription refuses to run"
$D -c "CREATE SUBSCRIPTION $SUB $CONN WITH (slot_name='$SLOT')" 2>&1 | grep -E '^ERROR' | head -2
[ "$(reg unsupported_tables)" = "error" ] && ok "registry records the default" \
  || fail "default mode is $(reg unsupported_tables)"
waitfor f 60 "SELECT enabled FROM pg_catalog.gp_mpp_subscription WHERE subname='$SUB'" \
  || fail "the subscription did not disable itself"
reason=$(reg "coalesce(disable_reason,'-')")
echo "  reason: $reason"
echo "$reason" | grep -q 'a1_ao' && echo "$reason" | grep -q 'ao_row' \
  && ok "the reason names the table and its access method" \
  || fail "the reason does not name the table: $reason"
[ "$($D -c "SELECT count(*) FROM pg_catalog.gp_subscription_rel WHERE subid=$(subid)")" = "0" ] \
  && ok "nothing was copied" || fail "state rows left behind: $(states)"

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. unsupported_tables = 'snapshot'"
$D -c "ALTER SUBSCRIPTION $SUB SET (unsupported_tables='snapshot')"
[ "$(reg unsupported_tables)" = "snapshot" ] && ok "ALTER ... SET changed the mode" \
  || fail "mode is $(reg unsupported_tables)"
$D -c "ALTER SUBSCRIPTION $SUB ENABLE"
waitfor 100 60 "SELECT count(*) FROM a1_h"  || fail "the heap table was not copied"
waitfor 100 30 "SELECT count(*) FROM a1_ao" || fail "the AO table was not copied"
echo "  states: $(states)"
[ "$(relstate a1_h)"  = "r" ] && ok "the heap table is streaming ('r')" || fail "a1_h is $(relstate a1_h)"
[ "$(relstate a1_ao)" = "s" ] && ok "the AO table is a snapshot ('s')"  || fail "a1_ao is $(relstate a1_ao)"

echo "-- both tables change on the source: only the heap one may follow"
$Q -c "INSERT INTO a1_h  SELECT g,'new' FROM generate_series(101,150) g" >/dev/null
$Q -c "INSERT INTO a1_ao SELECT g,'new' FROM generate_series(101,150) g" >/dev/null
waitfor 150 60 "SELECT count(*) FROM a1_h" || fail "the heap table stopped following"
[ "$($D -c "SELECT count(*) FROM a1_ao")" = "100" ] \
  && ok "the AO table stayed as it was copied (100 of 150 rows)" \
  || fail "the AO table changed: $($D -c "SELECT count(*) FROM a1_ao") rows"
[ "$(reg enabled)" = "t" ] && ok "the subscription is still enabled" \
  || fail "the subscription was disabled: $(reg "coalesce(disable_reason,'-')")"

# ------------------------------------------------------------------ phase 4
echo; echo "==== 4. a table added to a running subscription"
$Q -c "CREATE TABLE a1_ao2 (id int PRIMARY KEY, v text) USING ao_column DISTRIBUTED BY (id)"
$Q -c "INSERT INTO a1_ao2 SELECT g,'seed' FROM generate_series(1,40) g"
$D -c "CREATE TABLE a1_ao2 (id int PRIMARY KEY, v text) USING ao_column DISTRIBUTED BY (id)"
$Q -c "ALTER PUBLICATION $PUB ADD TABLE a1_ao2"

$D -c "ALTER SUBSCRIPTION $SUB SET (unsupported_tables='error')"
out=$($D -c "ALTER SUBSCRIPTION $SUB REFRESH PUBLICATION" 2>&1)
echo "$out" | grep -E '^ERROR' | head -1
echo "$out" | grep -q '^ERROR.*not decoded.*a1_ao2 (ao_column)' \
  && ok "REFRESH is refused on the spot, naming the table" || fail "REFRESH was not refused: $out"
[ -z "$(relstate a1_ao2)" ] && ok "no state row was written" || fail "a1_ao2 is $(relstate a1_ao2)"

$D -c "ALTER SUBSCRIPTION $SUB SET (unsupported_tables='snapshot')"
out=$($D -c "ALTER SUBSCRIPTION $SUB REFRESH PUBLICATION" 2>&1)
echo "$out" | grep -E '^WARNING|^NOTICE' | head -2
echo "$out" | grep -q '^WARNING.*not decoded' && ok "REFRESH warns instead" || fail "no warning: $out"
waitfor 40 60 "SELECT count(*) FROM a1_ao2" || fail "the added table was not copied"
waitfor s  30 "SELECT state FROM pg_catalog.gp_subscription_rel
                WHERE subid=$(subid) AND relid='a1_ao2'::regclass" \
  || fail "the added table did not end in state 's'"
echo "  states: $(states)"
$Q -c "INSERT INTO a1_h SELECT g,'p4' FROM generate_series(200,210) g" >/dev/null
waitfor 161 60 "SELECT count(*) FROM a1_h" || fail "the heap table stopped following after REFRESH"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
