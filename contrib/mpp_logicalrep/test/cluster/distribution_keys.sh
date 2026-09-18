#!/bin/bash
# The target's distribution key is not the source's.
#
# A change that moves a row to another target segment must be applied as a
# DELETE on the old segment plus an INSERT on the new one (mpp_route.c), and
# every row must end up on the segment its own key hashes to.  The shapes
# differ in what the source publishes as replica identity, which decides
# whether the old distribution key is knowable at all.
#
#   c  both sides REPLICA IDENTITY FULL, no primary key
#   k  primary key (v1,v2) on both sides: the identity covers the target's
#      distribution key, so an UPDATE of v2 changes the key and carries the
#      old tuple
#   n  source identity is the primary key (v1) alone, which does not cover
#      the target's distribution key: unsupported, must fail loudly
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
PUB=dk_pub; SUB=dk_sub; SLOT=dk_slot
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }

ck()  { psql -p "$1" -d postgres -Atq -c "SELECT count(*)||'/'||coalesce(sum(hashtext(x::text))::bigint,0) FROM $2 x"; }
wait_match() {
  local tbl=$1 s t
  for k in $(seq 1 ${2:-30}); do
    s=$(ck $SRC_PORT $tbl); t=$(ck $TGT_PORT $tbl)
    [ "$s" = "$t" ] && { echo "  $tbl in sync at +${k}s ($s)"; return 0; }
    sleep 1
  done
  echo "  $tbl NOT in sync after ${2:-30}s: source=$s target=$t"; return 1
}

# every row on the segment its own distribution key hashes to: compare the
# per-segment shape with a reference table of the same policy and contents
placement_ok() {
  local tbl=$1 ref=${1}_ref a b
  $T -c "DROP TABLE IF EXISTS $ref" >/dev/null 2>&1
  $T -c "CREATE TABLE $ref (LIKE $tbl) DISTRIBUTED BY ($2)" >/dev/null
  $T -c "INSERT INTO $ref SELECT * FROM $tbl" >/dev/null
  # compare row by row, not as an aggregated string: string_agg(... ORDER BY)
  # is assembled per segment and comes back in an arbitrary order
  a=$($T -c "SELECT count(*) FROM ((SELECT gp_segment_id, count(*) n FROM $tbl GROUP BY 1) EXCEPT ALL (SELECT gp_segment_id, count(*) n FROM $ref GROUP BY 1)) d")
  b=0
  [ "$a" != "0" ] && a=$($T -c "SELECT string_agg(gp_segment_id||':'||n, ' ') FROM (SELECT gp_segment_id, count(*) n FROM $tbl GROUP BY 1) x") && b=$($T -c "SELECT string_agg(gp_segment_id||':'||n, ' ') FROM (SELECT gp_segment_id, count(*) n FROM $ref GROUP BY 1) x")
  $T -c "DROP TABLE $ref" >/dev/null
  [ "$a" = "0" ] && { ok "$tbl rows sit on the right segments"; return 0; }
  fail "$tbl segment placement: table=$a reference=$b"
}

# gpconfig/gpstop act on whatever cluster the environment points at, which is
# not necessarily the target: say which one explicitly
TGT_DATADIR=$($T -c "SHOW data_directory")
gpt() { COORDINATOR_DATA_DIRECTORY="$TGT_DATADIR" MASTER_DATA_DIRECTORY="$TGT_DATADIR" PGPORT=$TGT_PORT "$@"; }

# the worker's warnings land in the target coordinator log
warned() {
  local L pat="$1"
  L=$(ls -t $($T -c "SHOW data_directory")/log/*.csv | head -1)
  grep -q "$pat" "$L"
}

cleanup() {
  $T -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
  $T -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  for k in $(seq 1 30); do
    [ "$($T -c "SELECT count(*) FROM pg_stat_activity WHERE application_name='mpp_apply_worker'")" = "0" ] && break
    sleep 0.5
  done
  $S -c "DROP PUBLICATION IF EXISTS $PUB" >/dev/null 2>&1
  for k in $(seq 1 20); do
    $S -c "SELECT pg_catalog.gp_drop_replication_slot('$SLOT')" >/dev/null 2>&1
    [ "$($S -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break
    sleep 0.5
  done
  for t in dk_c dk_k dk_n dk_m; do
    $S -c "DROP TABLE IF EXISTS $t" >/dev/null 2>&1
    $T -c "DROP TABLE IF EXISTS $t" >/dev/null 2>&1
  done
}

echo "==== source :$SRC_PORT -> target :$TGT_PORT"
echo "==== cleanup"; cleanup

# one failed attempt costs about ten seconds, so do not make phase 7 wait for
# the default ten of them
gpt gpconfig -c mpp_logicalrep.apply_max_retries -v 3 >/dev/null 2>&1 || fail "gpconfig could not set the retry count"
gpt gpstop -u -q -a >/dev/null 2>&1 || true

# ------------------------------------------------------------------ schema
# c: no key at all, both sides FULL
$S -c "CREATE TABLE dk_c (v1 int, v2 int, pad text) DISTRIBUTED BY (v1)" >/dev/null
$S -c "ALTER TABLE dk_c REPLICA IDENTITY FULL" >/dev/null
$T -c "CREATE TABLE dk_c (v1 int, v2 int, pad text) DISTRIBUTED BY (v2)" >/dev/null
$T -c "ALTER TABLE dk_c REPLICA IDENTITY FULL" >/dev/null
# k: the key covers both distribution keys
$S -c "CREATE TABLE dk_k (v1 int, v2 int, pad text, PRIMARY KEY (v1, v2)) DISTRIBUTED BY (v1)" >/dev/null
$T -c "CREATE TABLE dk_k (v1 int, v2 int, pad text, PRIMARY KEY (v1, v2)) DISTRIBUTED BY (v2)" >/dev/null

for t in dk_c dk_k; do
  $S -c "INSERT INTO $t SELECT g, g, 'seed' FROM generate_series(1,200) g" >/dev/null
done
$S -c "CREATE PUBLICATION $PUB FOR TABLE dk_c, dk_k" >/dev/null
$T -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$SRC_PORT dbname=postgres' PUBLICATION $PUB WITH (slot_name='$SLOT')" 2>&1 | grep -vE '^NOTICE|^WARNING|^HINT|^$' | head -2

echo; echo "==== 1. initial sync"
wait_match dk_c 60 || fail "dk_c initial sync"
wait_match dk_k 60 || fail "dk_k initial sync"
placement_ok dk_c v2
placement_ok dk_k v2

echo; echo "==== 2. UPDATE that moves the row to another target segment"
$S -c "UPDATE dk_c SET v2 = v2 + 1000 WHERE v1 <= 50" >/dev/null
$S -c "UPDATE dk_k SET v2 = v2 + 1000 WHERE v1 <= 50" >/dev/null
wait_match dk_c 60 || fail "dk_c after moving UPDATE"
wait_match dk_k 60 || fail "dk_k after moving UPDATE"
placement_ok dk_c v2
placement_ok dk_k v2

echo; echo "==== 3. UPDATE that does not move the row"
$S -c "UPDATE dk_c SET pad = 'p' WHERE v1 BETWEEN 60 AND 90" >/dev/null
$S -c "UPDATE dk_k SET pad = 'p' WHERE v1 BETWEEN 60 AND 90" >/dev/null
wait_match dk_c 60 || fail "dk_c after non-moving UPDATE"
wait_match dk_k 60 || fail "dk_k after non-moving UPDATE"

echo; echo "==== 4. DELETE of rows that moved, and of rows that did not"
$S -c "DELETE FROM dk_c WHERE v1 <= 20 OR v1 BETWEEN 100 AND 120" >/dev/null
$S -c "DELETE FROM dk_k WHERE v1 <= 20 OR v1 BETWEEN 100 AND 120" >/dev/null
wait_match dk_c 60 || fail "dk_c after DELETE"
wait_match dk_k 60 || fail "dk_k after DELETE"
placement_ok dk_c v2
placement_ok dk_k v2

echo; echo "==== 5. moving UPDATE under a mixed load, in one transaction"
$S -c "BEGIN;
       UPDATE dk_c SET v2 = v2 + 10000 WHERE v1 BETWEEN 130 AND 160;
       INSERT INTO dk_c VALUES (500, 500, 'new'), (501, 501, 'new');
       DELETE FROM dk_c WHERE v1 = 131;
       UPDATE dk_k SET v2 = v2 + 10000 WHERE v1 BETWEEN 130 AND 160;
       INSERT INTO dk_k VALUES (500, 500, 'new'), (501, 501, 'new');
       DELETE FROM dk_k WHERE v1 = 131;
       COMMIT" >/dev/null
wait_match dk_c 60 || fail "dk_c after the mixed transaction"
wait_match dk_k 60 || fail "dk_k after the mixed transaction"
placement_ok dk_c v2
placement_ok dk_k v2

echo; echo "==== 6. a source distribution key that the target cannot tell apart"
# (kept before the dk_n case: that one leaves the subscription retrying an
#  UPDATE it can never apply, so nothing added afterwards would be copied)
# dk_m: the source distributes by v1 and the target's identity is v2 alone.
# Updating v1 on the source moves the row between source segments, which
# reaches the target as a DELETE from one stream and an INSERT from another,
# of what the target considers the same row -- in no defined order.
$S -c "CREATE TABLE dk_m (v1 int, v2 int, pad text, PRIMARY KEY (v1, v2)) DISTRIBUTED BY (v1)" >/dev/null
$T -c "CREATE TABLE dk_m (v1 int, v2 int, pad text, PRIMARY KEY (v2)) DISTRIBUTED BY (v2)" >/dev/null
$S -c "INSERT INTO dk_m SELECT g, g, 'seed' FROM generate_series(1,100) g" >/dev/null
$S -c "ALTER PUBLICATION $PUB ADD TABLE dk_m" >/dev/null
$T -c "ALTER SUBSCRIPTION $SUB REFRESH PUBLICATION" >/dev/null 2>&1
wait_match dk_m 60 || fail "dk_m initial sync"
warned "distribution key .* of source relation .* is not part of the replica identity of the target" \
  && ok "warned about the source's distribution key" \
  || fail "no warning about the source's distribution key"

# changes that leave the source's distribution key alone are fine
$S -c "UPDATE dk_m SET pad = 'p' WHERE v2 <= 30" >/dev/null
wait_match dk_m 60 || fail "dk_m after an UPDATE that does not move the row"

echo; echo "==== 7. a source identity that does not cover the target's distribution key"
# dk_n: source publishes the primary key (v1) only, target distributes by v2.
# The old v2 is then unknowable, so this cannot be applied correctly -- what
# matters is that it says so instead of losing the change.
$S -c "CREATE TABLE dk_n (v1 int PRIMARY KEY, v2 int) DISTRIBUTED BY (v1)" >/dev/null
$T -c "CREATE TABLE dk_n (v1 int, v2 int) DISTRIBUTED BY (v2)" >/dev/null
$T -c "ALTER TABLE dk_n REPLICA IDENTITY FULL" >/dev/null
$S -c "INSERT INTO dk_n SELECT g, g FROM generate_series(1,10) g" >/dev/null
$S -c "ALTER PUBLICATION $PUB ADD TABLE dk_n" >/dev/null
$T -c "ALTER SUBSCRIPTION $SUB REFRESH PUBLICATION" >/dev/null 2>&1
wait_match dk_n 60 || fail "dk_n initial sync"
# wait for the table to be ready, not merely equal: an UPDATE that arrives
# while the copy is still running is held, and the error it raises at
# catch-up time abandons the sync -- which is then redone from a later cut
# point that already contains the UPDATE, and nothing looks wrong
for k in $(seq 1 60); do
  [ "$($T -c "SELECT state FROM pg_catalog.gp_subscription_rel WHERE relid = to_regclass('dk_n')::oid")" = "r" ] && break
  sleep 1
done
[ "$($T -c "SELECT state FROM pg_catalog.gp_subscription_rel WHERE relid = to_regclass('dk_n')::oid")" = "r" ] \
  || fail "dk_n never became ready"
$S -c "UPDATE dk_n SET v2 = v2 + 1000 WHERE v1 <= 3" >/dev/null
sleep 8
if [ "$(ck $SRC_PORT dk_n)" = "$(ck $TGT_PORT dk_n)" ]; then
  fail "dk_n converged, which it cannot: the old distribution key is not in the stream"
else
  echo "  dk_n did not converge, as it cannot: source=$(ck $SRC_PORT dk_n) target=$(ck $TGT_PORT dk_n)"
  # the change must be refused, not dropped: the coordinator logs what the
  # reader or the writer raised
  L=$(ls -t $($T -c "SHOW data_directory")/log/*.csv | head -1)
  # the CSV log escapes the quotes around the relation name, so match on the
  # part of the message before it
  msg=$(grep -ho 'recovering after: .\{0,200\}' "$L" | tail -1)
  echo "  worker: $msg"
  case "$msg" in
    *"cannot apply UPDATE"*|*"distribution key is not part of the replica identity"*)
      ok "refused with the distribution-key reason" ;;
    *"REPLICA IDENTITY"*|*"replica identity"*)
      ok "refused (replica identity mismatch)" ;;
    *)  fail "dk_n: the change was dropped without an error" ;;
  esac
fi

warned "distribution key .* of target relation .* is not part of the replica identity of the source" \
  && ok "warned about it when the table was added" \
  || fail "no warning about the target's distribution key"

# O6: an error that comes back on every replay must stop the subscription
# instead of retrying for ever (and blocking everything else with it).
# not pg_subscription.subenabled: that is false for every one of our
# subscriptions from the moment it is created.  The registry is the state.
enabled_now() { $T -c "SELECT enabled FROM pg_catalog.gp_mpp_subscription WHERE subname='$SUB'"; }
for k in $(seq 1 90); do
  [ "$(enabled_now)" = "f" ] && break
  sleep 1
done
if [ "$(enabled_now)" = "f" ]; then
  ok "subscription disabled itself after the repeated failure (+${k}s)"
  reason=$($T -c "SELECT disable_reason FROM pg_catalog.gp_mpp_subscription WHERE subname='$SUB'")
  case "$reason" in
    *"distribution key"*) ok "said why: $reason" ;;
    "")                   fail "no reason recorded" ;;
    *)                    fail "unexpected reason: $reason" ;;
  esac
else
  fail "subscription kept retrying an error that cannot pass"
fi
gpt gpconfig -c mpp_logicalrep.apply_max_retries -v 10 >/dev/null 2>&1
gpt gpstop -u -q -a >/dev/null 2>&1 || true

echo; echo "==== cleanup"; cleanup
[ $FAILS -eq 0 ] && echo "ALL PASS" || echo "$FAILS FAILURE(S)"
exit $([ $FAILS -eq 0 ] && echo 0 || echo 1)
