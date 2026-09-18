#!/bin/bash
# gp_stat_mpp_subscription and gp_stat_mpp_subscription_segments: what a
# running worker publishes about itself, and what is left to see once it has
# stopped.
#
#   1  no worker: the subscription is still in the view, with its registry
#      columns and null live ones
#   2  a running worker: phase, positions, table states, counters
#   3  under load: the counters move and the lag is visible; the per-segment
#      view has a row per source segment and agrees with the state table
#   4  the worker stops: the live columns go null again, the positions stay
#   5  the launcher is woken by ENABLE rather than slept out
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
SLOT=o2_slot; PUB=o2_pub; SUB=o2_sub; TBL=o2_t
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS+1)); }
ok()   { echo "  ok: $*"; }
v()    { $D -c "SELECT $1 FROM pg_catalog.gp_stat_mpp_subscription WHERE subname='$SUB'"; }
segs() { $D -c "SELECT $1 FROM pg_catalog.gp_stat_mpp_subscription_segments WHERE subname='$SUB'"; }
workers() { $D -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='mpp apply worker'"; }
quiesce() { for k in $(seq 1 60); do [ "$(workers)" = "0" ] && return 0; sleep 0.5; done; return 1; }
waitfor() {  # <want> <seconds> <sql on lrdst>
  local want=$1 n=$2 sql=$3 got=
  for k in $(seq 1 "$n"); do
    got=$($D -c "$sql"); [ "$got" = "$want" ] && return 0; sleep 1
  done
  echo "  ($sql) = $got after $n s, wanted $want"; return 1
}

cleanup() {
  $D -c "ALTER SUBSCRIPTION $SUB DISABLE" >/dev/null 2>&1
  $D -c "DROP SUBSCRIPTION IF EXISTS $SUB" >/dev/null 2>&1
  quiesce
  for k in $(seq 1 20); do
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name='$SLOT'" >/dev/null 2>&1
    $Q -c "SELECT pg_drop_replication_slot(slot_name) FROM gp_dist_random('pg_replication_slots') WHERE slot_name='$SLOT'" >/dev/null 2>&1
    [ "$($Q -c "SELECT count(*) FROM gp_replication_slots WHERE slot_name='$SLOT'")" = "0" ] && break
    sleep 0.5
  done
  $Q -c "DROP PUBLICATION IF EXISTS $PUB; DROP TABLE IF EXISTS $TBL" >/dev/null 2>&1
  $D -c "DROP TABLE IF EXISTS $TBL" >/dev/null 2>&1
}

echo "==== cluster :$PORT, postgres -> lrdst"
echo "==== cleanup"; cleanup

NSEG=$($Q -c "SELECT count(*) FROM gp_segment_configuration WHERE content >= 0 AND role = 'p'")
echo "-- source has $NSEG primary segments"
$Q -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "INSERT INTO $TBL SELECT g,'seed' FROM generate_series(1,20000) g"
$Q -c "CREATE PUBLICATION $PUB FOR TABLE $TBL"
$D -c "CREATE TABLE $TBL (id int PRIMARY KEY, v text) DISTRIBUTED BY (id)"
$Q -c "SELECT count(*) FROM gp_create_logical_replication_slot('$SLOT','pgoutput',false,true)" >/dev/null

# ------------------------------------------------------------------ phase 1
echo; echo "==== 1. created but not enabled"
$D -c "CREATE SUBSCRIPTION $SUB CONNECTION 'host=localhost port=$PORT dbname=postgres' PUBLICATION $PUB WITH (slot_name='$SLOT', enabled=false)" 2>&1 | grep -E '^ERROR' | head -2
[ "$(v 'count(*)')" = "1" ] && ok "the subscription is in the view" || fail "not in the view"
[ "$(v 'phase')" = "not running" ] && ok "phase is 'not running'" || fail "phase is $(v 'phase')"
[ "$(v 'pid IS NULL')" = "t" ] && ok "no pid" || fail "pid is $(v pid)"
[ "$(v 'enabled')" = "f" ] && ok "the registry columns are there" || fail "enabled is $(v enabled)"
[ "$(segs 'count(*)')" = "0" ] && ok "no segment rows yet" || fail "segment rows: $(segs 'count(*)')"

# ------------------------------------------------------------------ phase 2
echo; echo "==== 2. enabled: the launcher is woken, not slept out"
NAP=$($D -c "SELECT setting FROM pg_settings WHERE name='mpp_logicalrep.launcher_naptime_ms'")
T0=$($D -c "SELECT extract(epoch FROM clock_timestamp())")
$D -c "ALTER SUBSCRIPTION $SUB ENABLE"
for k in $(seq 1 600); do [ "$(workers)" != "0" ] && break; sleep 0.05; done
T1=$($D -c "SELECT extract(epoch FROM clock_timestamp())")
MS=$($D -c "SELECT round(($T1 - $T0) * 1000)")
echo "  worker started ${MS} ms after ENABLE (naptime ${NAP} ms)"
[ "$MS" -lt "$NAP" ] && ok "faster than a whole nap" || fail "took a whole nap: ${MS} >= ${NAP} ms"

waitfor 20000 60 "SELECT count(*) FROM $TBL" || fail "the initial copy did not finish"
[ "$(v 'phase')" = "streaming" ] && ok "phase is 'streaming'" || fail "phase is $(v 'phase')"
[ "$(v 'source_segments')" = "$NSEG" ] && ok "source_segments = $NSEG" || fail "source_segments is $(v 'source_segments')"
[ "$(v 'tables')" = "1" ] && [ "$(v 'tables_ready')" = "1" ] \
  && ok "one table, ready" || fail "tables=$(v tables) ready=$(v tables_ready)"
[ "$(segs 'count(*)')" = "$NSEG" ] && ok "one segment row per source segment" \
  || fail "segment rows: $(segs 'count(*)')"

# ------------------------------------------------------------------ phase 3
echo; echo "==== 3. under load"
B0=$(v batches); R0=$(v rows_applied)
( b=100; end=$((SECONDS+15)); while [ $SECONDS -lt $end ]; do b=$((b+1)); base=$((b*1000))
  $Q -c "INSERT INTO $TBL SELECT g,'w$b' FROM generate_series($base,$((base+49))) g" >/dev/null 2>&1
  $Q -c "UPDATE $TBL SET v='u$b' WHERE id=$((b*7))" >/dev/null 2>&1
  $Q -c "DELETE FROM $TBL WHERE id=$((b*13))" >/dev/null 2>&1
done ) &
WL=$!
sleep 8
echo "-- a look while it runs:"
$D -c "\\x on" -c "SELECT phase, received_lsn, applied_lsn, apply_lag_bytes, batch_txns, buffered_txns, buffered_bytes, events, batches, txns_applied, rows_applied, recoveries, stuck_errors FROM pg_catalog.gp_stat_mpp_subscription WHERE subname='$SUB'" 2>/dev/null | sed 's/^/  /'
[ "$(v 'received_lsn IS NOT NULL')" = "t" ] && ok "received_lsn is set" || fail "received_lsn is null"
[ "$(v 'apply_lag_bytes IS NOT NULL')" = "t" ] && ok "the lag is a number" || fail "lag is null"
wait $WL
waitfor "$($Q -c "SELECT count(*) FROM $TBL")" 60 "SELECT count(*) FROM $TBL" || fail "did not catch up"
B1=$(v batches); R1=$(v rows_applied)
[ "$B1" -gt "$B0" ] && ok "batches moved: $B0 -> $B1" || fail "batches did not move ($B0)"
[ "$R1" -gt "$R0" ] && ok "rows_applied moved: $R0 -> $R1" || fail "rows_applied did not move ($R0)"
[ "$(v recoveries)" = "0" ] && ok "no recoveries" || fail "recoveries: $(v recoveries)"

# the per-segment view has to say what the state table says: the live
# positions and the ones the last committed batch persisted are the same
# reading, taken from two places.
SEEN=$(segs "array_agg(pos::text ORDER BY source_segment)::text")
KEPT=$($D -c "SELECT array_agg(('0/0'::pg_lsn + u.p)::text ORDER BY u.i)::text
                FROM pg_catalog.gp_mpp_subscription s
                JOIN pg_catalog.gp_subscription_progress g ON g.subid = s.subid,
                     LATERAL unnest(g.seg_pos) WITH ORDINALITY AS u(p, i)
               WHERE s.subname='$SUB'")
[ -n "$KEPT" ] && [ "$SEEN" = "$KEPT" ] && ok "the view agrees with gp_subscription_progress: $SEEN" \
  || fail "view $SEEN vs state table $KEPT"
echo "-- per segment:"; $D -c "SELECT source_segment, pos, flush, buffered_txns, buffered_bytes, applied_txns FROM pg_catalog.gp_stat_mpp_subscription_segments WHERE subname='$SUB' ORDER BY 1" 2>/dev/null | sed 's/^/  /'

# ------------------------------------------------------------------ phase 4
echo; echo "==== 4. stopped: the live columns go, the positions stay"
POS=$(segs 'array_agg(pos ORDER BY source_segment)::text')
$D -c "ALTER SUBSCRIPTION $SUB DISABLE"; quiesce || fail "the worker did not stop"
[ "$(v 'phase')" = "not running" ] && ok "phase is 'not running' again" || fail "phase is $(v 'phase')"
[ "$(v 'pid IS NULL')" = "t" ] && ok "no pid" || fail "pid is $(v pid)"
[ "$(v 'applied_lsn IS NOT NULL')" = "t" ] && ok "applied_lsn survives in the state table" \
  || fail "applied_lsn is null after the worker went"
[ "$(segs 'count(*)')" = "$NSEG" ] && ok "the segment rows are still there" \
  || fail "segment rows: $(segs 'count(*)')"
[ "$(segs 'array_agg(pos ORDER BY source_segment)::text')" = "$POS" ] \
  && ok "and hold the positions the last batch left" || fail "positions changed: $POS -> $(segs 'array_agg(pos ORDER BY source_segment)::text')"
[ "$(segs 'count(*) FILTER (WHERE buffered_txns IS NULL)')" = "$NSEG" ] \
  && ok "nothing is buffered any more" || fail "buffered_txns still reported"

echo; echo "==== cleanup"; cleanup
echo; [ $FAILS -eq 0 ] && { echo "ALL PASS"; exit 0; } || { echo "$FAILS FAILURE(S)"; exit 1; }
