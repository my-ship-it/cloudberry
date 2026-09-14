#!/bin/bash
# The source's replication slots are gone (a segment failover takes them with
# it) -- the subscription must not sit in a recovery loop.
#
#   1  gp_drop_replication_slot() is idempotent (absent slot, half-dropped)
#   2  slot_loss_action = reinit (the default): the worker notices the slots
#      are gone, redoes the whole subscription and converges -- including the
#      rows written while there was no slot at all
#   3  slot_loss_action = disable: the worker disables the subscription and
#      says why instead of resynchronising
set -u
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null
SRC_PORT=7100; TGT_PORT=7300
while getopts "s:t:" opt; do
  case $opt in
    s) SRC_PORT=$OPTARG ;; t) TGT_PORT=$OPTARG ;;
    *) echo "usage: $0 [-s <source port>] [-t <target port>]"; exit 2 ;;
  esac
done
S="psql -p $SRC_PORT -d postgres -Atq -v ON_ERROR_STOP=0"
T="psql -p $TGT_PORT -d postgres -Atq -v ON_ERROR_STOP=0"
SLOT=o1_slot; PUB=o1_pub; SUB=o1_sub; TBL=o1_t
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }
ck() { psql -p "$1" -d postgres -Atq -c "SELECT count(*)||'/'||coalesce(sum(hashtext(x::text))::bigint,0) FROM $TBL x"; }
wait_match() {
  local s t
  for k in $(seq 1 $1); do
    s=$(ck $SRC_PORT); t=$(ck $TGT_PORT)
    [ "$s" = "$t" ] && { echo "  in sync at +${k}s ($s)"; return 0; }
    sleep 1
  done
  echo "  NOT in sync after $1 s: source=$s target=$t"; return 1
}
tgt_log() { ls -t $($T -c "SHOW data_directory")/log/*.csv | head -1; }
# gpconfig/gpstop act on whatever cluster the environment points at, which is
# not necessarily the target: say which one explicitly.
TGT_DATADIR=$($T -c "SHOW data_directory")
gpt() { COORDINATOR_DATA_DIRECTORY="$TGT_DATADIR" MASTER_DATA_DIRECTORY="$TGT_DATADIR" PGPORT=$TGT_PORT "$@"; }
drop_slot_everywhere() {   # what an operator would do
  $S -c "SELECT gp_drop_replication_slot('$SLOT')" >/dev/null 2>&1
}
cleanup() {
  $T -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
  $T -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  for k in $(seq 1 30); do
    n=$($T -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='logical replication worker' OR application_name IN ('mpp_apply_worker','mpp_apply_sync')")
    [ "$n" = "0" ] && break; sleep 0.5
  done
  for k in $(seq 1 10); do drop_slot_everywhere; [ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break; sleep 0.5; done
  $S -c "DROP PUBLICATION IF EXISTS $PUB; DROP TABLE IF EXISTS $TBL CASCADE" >/dev/null 2>&1
  $T -c "DROP TABLE IF EXISTS $TBL CASCADE" >/dev/null 2>&1
}

echo "==== source :$SRC_PORT -> target :$TGT_PORT"
echo "==== cleanup"; cleanup
MARK=$(wc -l < "$(tgt_log)")

# ------------------------------------------------------------------ phase 1
echo; echo "==== 1. gp_drop_replication_slot() is idempotent"
$S -c "SELECT count(*) FROM gp_create_logical_replication_slot('$SLOT','pgoutput',false,true)" >/dev/null
n=$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")
[ "$n" -gt 0 ] && ok "slot created on $n nodes" || fail "slot not created"
# drop only the coordinator's copy, leaving the half-dropped state that used to wedge it
$S -c "SELECT pg_drop_replication_slot('$SLOT')" >/dev/null 2>&1
out=$($S -c "SELECT gp_drop_replication_slot('$SLOT')" 2>&1)
echo "$out" | grep -qi 'does not exist' && fail "half-dropped state still errors: $out" || ok "half-dropped state drops cleanly"
out=$($S -c "SELECT gp_drop_replication_slot('$SLOT')" 2>&1)
echo "$out" | grep -qi 'does not exist' && fail "second call errors: $out" || ok "second call on an absent slot is a no-op"
[ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && ok "no slot left" || fail "slots left"

# ------------------------------------------------------------------ phase 2
echo; echo "==== 2. slots lost -> reinit (default)"
$S -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)" >/dev/null
$S -c "INSERT INTO $TBL SELECT g,'seed' FROM generate_series(1,2000) g" >/dev/null
$S -c "CREATE PUBLICATION $PUB FOR TABLE $TBL" >/dev/null
$T -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)" >/dev/null
$T -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$SRC_PORT dbname=postgres' PUBLICATION $PUB WITH (mpp=on, slot_name='$SLOT')" 2>&1 | grep -vE '^NOTICE|^WARNING|^HINT|^$' | head -2
wait_match 60 || fail "initial sync did not converge"
$S -c "INSERT INTO $TBL SELECT g,'live' FROM generate_series(3001,3100) g" >/dev/null
wait_match 60 || fail "streaming did not converge"

echo "-- taking the slots away (what a segment failover does)"
$T -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null
for k in $(seq 1 40); do
  [ "$($T -c "SELECT count(*) FROM pg_stat_activity WHERE application_name='mpp_apply_worker'")" = "0" ] && break; sleep 0.5
done
for k in $(seq 1 10); do drop_slot_everywhere; [ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break; sleep 0.5; done
[ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && ok "slots gone" || fail "slots still there"
# these rows exist only in the table now: no slot holds their WAL
$S -c "INSERT INTO $TBL SELECT g,'gap' FROM generate_series(5001,5300) g" >/dev/null
$S -c "DELETE FROM $TBL WHERE id BETWEEN 1 AND 50" >/dev/null
echo "-- re-enabling"
$T -c "ALTER SUBSCRIPTION $SUB ENABLE" >/dev/null
wait_match 120 || fail "did not converge after the slot loss"
grep -q 'redoing the initial synchronisation of subscription' "$(tgt_log)" && ok "worker reported the slot loss and redid the sync" || fail "no slot-loss message in the log"
[ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" -gt 0 ] && ok "slots recreated" || fail "slots not recreated"

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. slots lost -> disable"
gpt gpconfig -c mpp_logicalrep.slot_loss_action -v disable >/dev/null 2>&1 || fail "gpconfig could not set the GUC on the target"
gpt gpstop -u -q -a >/dev/null 2>&1 || true
sleep 2
$T -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null
for k in $(seq 1 40); do
  [ "$($T -c "SELECT count(*) FROM pg_stat_activity WHERE application_name='mpp_apply_worker'")" = "0" ] && break; sleep 0.5
done
for k in $(seq 1 10); do drop_slot_everywhere; [ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break; sleep 0.5; done
$T -c "ALTER SUBSCRIPTION $SUB ENABLE" >/dev/null
# pg_subscription.subenabled is always false for our subscriptions -- that is
# what keeps the server's own launcher off them -- so ask the registry.
enabled_now() { $T -c "SELECT enabled FROM pg_catalog.gp_mpp_subscription WHERE subname='$SUB'"; }
for k in $(seq 1 60); do [ "$(enabled_now)" = "f" ] && break; sleep 1; done
[ "$(enabled_now)" = "f" ] \
  && ok "subscription disabled itself (+${k}s)" || fail "subscription was not disabled"
reason=$($T -c "SELECT disable_reason FROM pg_catalog.gp_mpp_subscription WHERE subname='$SUB'")
[ -n "$reason" ] && ok "reason recorded: $reason" || fail "no reason recorded"
gpt gpconfig -c mpp_logicalrep.slot_loss_action -v reinit >/dev/null 2>&1
gpt gpstop -u -q -a >/dev/null 2>&1 || true

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
