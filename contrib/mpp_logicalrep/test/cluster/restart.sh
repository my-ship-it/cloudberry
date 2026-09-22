#!/bin/bash
# Stopping and starting the target cluster under a live subscription.
#
#   1  gpstop in its default smart mode finishes.  Smart shutdown waits for
#      every client backend to go, and the worker drives its gangs from an
#      ordinary session of its own, so it has to notice the shutdown and stop
#      itself or the wait never ends
#   2  gpstart brings the subscription back by itself -- enabled, streaming,
#      and caught up on what was written while the target was down
#   3  the same over a fast stop, which is the path an operator takes when a
#      smart one is not wanted
#
# 2 is the one that used to fail, and not because of anything about slots:
# gpstart brings a coordinator up in utility mode to do its own bootstrapping
# and stops it a second later, and the worker started in that one read the
# source as having lost most of its slots and disabled the subscription over
# it.  The launcher does not run there any more.
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
SLOT=rs_slot; PUB=rs_pub; SUB=rs_sub; TBL=rs_t
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
# gpstop/gpstart act on whatever cluster the environment points at, which is
# not necessarily the target: say which one explicitly.
TGT_DATADIR=$($T -c "SHOW data_directory")
gpt() { COORDINATOR_DATA_DIRECTORY="$TGT_DATADIR" MASTER_DATA_DIRECTORY="$TGT_DATADIR" PGPORT=$TGT_PORT "$@"; }
tgt_up() { for k in $(seq 1 120); do $T -c "SELECT 1" >/dev/null 2>&1 && return 0; sleep 1; done; return 1; }
sub_state() { $T -c "SELECT enabled||'/'||phase||'/'||coalesce(disable_reason,'-') FROM gp_stat_mpp_subscription WHERE subname='$SUB'"; }
sub_enabled() { $T -c "SELECT enabled FROM gp_stat_mpp_subscription WHERE subname='$SUB'"; }
cleanup() {
  $T -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  for k in $(seq 1 10); do
    $S -c "SELECT gp_drop_replication_slot('$SLOT')" >/dev/null 2>&1
    [ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break; sleep 0.5
  done
  $S -c "DROP PUBLICATION IF EXISTS $PUB; DROP TABLE IF EXISTS $TBL CASCADE" >/dev/null 2>&1
  $T -c "DROP TABLE IF EXISTS $TBL CASCADE" >/dev/null 2>&1
}

echo "==== source :$SRC_PORT -> target :$TGT_PORT"
echo "==== cleanup"; cleanup

echo; echo "==== 0. a subscription that is streaming"
$S -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)" >/dev/null
$S -c "INSERT INTO $TBL SELECT g,'seed' FROM generate_series(1,2000) g" >/dev/null
$S -c "CREATE PUBLICATION $PUB FOR TABLE $TBL" >/dev/null
$T -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)" >/dev/null
$T -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$SRC_PORT dbname=postgres' PUBLICATION $PUB WITH (slot_name='$SLOT')" 2>&1 | grep -vE '^NOTICE|^WARNING|^HINT|^$' | head -2
wait_match 60 || fail "initial sync did not converge"

# ------------------------------------------------------------------ phase 1
echo; echo "==== 1. gpstop in its default smart mode finishes"
t0=$(date +%s)
gpt timeout 180 gpstop -a >/dev/null 2>&1; rc=$?
t1=$(date +%s); took=$((t1-t0))
if [ $rc -eq 124 ]; then
  fail "smart gpstop still waiting after ${took}s -- the worker did not notice the shutdown"
  gpt gpstop -a -M fast >/dev/null 2>&1
elif [ $rc -ne 0 ]; then
  fail "smart gpstop failed (rc=$rc) after ${took}s"
else
  ok "smart gpstop finished in ${took}s"
fi

# written while the target is down: the slots hold this WAL
$S -c "INSERT INTO $TBL SELECT g,'while down' FROM generate_series(4001,4200) g" >/dev/null
$S -c "DELETE FROM $TBL WHERE id BETWEEN 1 AND 30" >/dev/null

# ------------------------------------------------------------------ phase 2
echo; echo "==== 2. gpstart brings the subscription back by itself"
gpt gpstart -a >/dev/null 2>&1
tgt_up || { fail "target did not come back up"; echo "$FAILS FAILURE(S)"; exit 1; }
st=$(sub_state)
[ "$(sub_enabled)" = "t" ] && ok "still enabled after the restart ($st)" \
  || fail "the restart disabled the subscription ($st)"
wait_match 120 || fail "did not catch up after the restart"
st=$(sub_state)
case "$st" in
  */streaming/*) ok "streaming again ($st)" ;;
  *)             fail "not streaming after the restart ($st)" ;;
esac

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. the same over a fast stop"
gpt gpstop -a -M fast >/dev/null 2>&1
$S -c "INSERT INTO $TBL SELECT g,'after fast' FROM generate_series(6001,6100) g" >/dev/null
gpt gpstart -a >/dev/null 2>&1
tgt_up || { fail "target did not come back up after the fast stop"; echo "$FAILS FAILURE(S)"; exit 1; }
st=$(sub_state)
[ "$(sub_enabled)" = "t" ] && ok "still enabled after the fast restart ($st)" \
  || fail "the fast restart disabled the subscription ($st)"
wait_match 120 || fail "did not catch up after the fast restart"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
