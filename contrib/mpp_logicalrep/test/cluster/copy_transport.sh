#!/bin/bash
# The segment-to-segment COPY transport (copyhelper/) with the planner's
# redistribution on top -- the transport initial synchronisation is built on.
#   src (filled) --COPY TO PROGRAM send ON SEGMENT--> recv helper
#     --> gp_lr_copy_recv() --> INSERT INTO tgt (Redistribute Motion) --> tgt
#
# Phases (all on one 3-segment demo cluster):
#   A  3 -> 3   plain: one COPY ... ON SEGMENT, each source seg feeds target seg
#   B  6 -> 3   emulated: two COPY ... ON SEGMENT runs (rows split by id parity);
#               the second run acts as source segments 3..5, so every target
#               receives two clients (nsources=6)
#   C  2 -> 3   emulated: nsources=2 so target seg 2 expects nothing and exits
#               at once; the two sends are driven by hand from COPY TO STDOUT
#               piped into "cbdb_lr_copyhelper send" (the way a real 2-segment
#               source's QEs would run it)
set -u
source ~/cloudberry-db-devel/cloudberry-env.sh 2>/dev/null
PORT=7100
while getopts "s:" opt; do
  case $opt in
    s) PORT=$OPTARG ;;
    *) echo "usage: $0 [-s <port>]   (one cluster: database postgres -> lrdst)"; exit 2 ;;
  esac
done
DB=postgres
PSQL="psql -p $PORT -d $DB -v ON_ERROR_STOP=1 -tA"
S=${SCRATCH:-/tmp}
FAILS=0

NSEG=$($PSQL -c "SELECT count(*) FROM gp_segment_configuration WHERE content>=0 AND role='p';")

# the transport's working directory on each primary: <datadir>/gp_lr
seg_dirs() { $PSQL -c "SELECT datadir||'/gp_lr' FROM gp_segment_configuration
                        WHERE content >= 0 AND role = 'p' ORDER BY content;"; }

# wait_targets JOB EXPECTED_ROWS -> echoes host:port list ordered by segid
wait_targets() {
  local job=$1 want=$2 t=""
  for i in $(seq 1 150); do
    t=$($PSQL -c "SELECT string_agg(a.address||':'||p.port, ',' ORDER BY p.segid)
                  FROM gp_lr_copy_ports('$job') p
                  JOIN gp_segment_configuration a ON a.content=p.segid AND a.role='p'
                  HAVING count(*) = $want;")
    [ -n "$t" ] && break
    sleep 0.2
  done
  echo "$t"
}

# start_recv JOB NSOURCES -> background INSERT ... SELECT gp_lr_copy_recv; pid in RECV_PID
start_recv() {
  local job=$1 nsrc=$2
  psql -p $PORT -d $DB -v ON_ERROR_STOP=1 -tA \
    -c "INSERT INTO t12_tgt SELECT * FROM gp_lr_copy_recv('t12_tgt'::regclass, '$job', $nsrc) AS x(id int, val text);" \
    > $S/t12_recv.out 2> $S/t12_recv.err &
  RECV_PID=$!
}

verify() {
  local tag=$1
  $PSQL <<SQL
SELECT '$tag tgt rows', count(*) FROM t12_tgt;
SELECT '$tag tgt seg spread', gp_segment_id, count(*) FROM t12_tgt GROUP BY 2 ORDER BY 2;
-- NB: md5(string_agg(... ORDER BY id)) is NOT deterministic under ORCA's
-- two-phase aggregate (segment partials are concatenated in arrival order),
-- so compare with an order-independent checksum plus EXCEPT both ways.
SELECT '$tag checksum src', count(*), sum(hashtext(id||'|'||val)::bigint) FROM t12_src;
SELECT '$tag checksum tgt', count(*), sum(hashtext(id||'|'||val)::bigint) FROM t12_tgt;
SELECT '$tag MATCH?' AS check,
  (SELECT count(*) FROM (SELECT * FROM t12_src EXCEPT ALL SELECT * FROM t12_tgt) x) = 0 AND
  (SELECT count(*) FROM (SELECT * FROM t12_tgt EXCEPT ALL SELECT * FROM t12_src) x) = 0 AS ok;
SQL
  local ok
  ok=$($PSQL -c "SELECT (SELECT count(*) FROM (SELECT * FROM t12_src EXCEPT ALL SELECT * FROM t12_tgt) x) = 0 AND
                        (SELECT count(*) FROM (SELECT * FROM t12_tgt EXCEPT ALL SELECT * FROM t12_src) x) = 0;")
  [ "$ok" = "t" ] || { echo "FAIL: $tag data mismatch"; FAILS=$((FAILS+1)); }
}

cleanup_helpers() {
  pkill -x cbdb_lr_copyhelper 2>/dev/null
  for d in $(seg_dirs); do rm -f "$d"/t12_* 2>/dev/null; done
}

echo "### setup ($NSEG target segments)"
$PSQL <<SQL
DROP TABLE IF EXISTS t12_src, t12_tgt;
CREATE TABLE t12_src(id int, val text) DISTRIBUTED BY (id);
CREATE TABLE t12_tgt(id int, val text) DISTRIBUTED BY (id);
INSERT INTO t12_src SELECT g, 'row-'||g||'-payload-'||md5(g::text) FROM generate_series(1,20000) g;
SELECT 'src rows', count(*) FROM t12_src;
SELECT 'src seg spread', gp_segment_id, count(*) FROM t12_src GROUP BY 2 ORDER BY 2;
SQL

# ---------------------------------------------------------------- phase A
JOB="t12a_$$"
echo; echo "### A: $NSEG -> $NSEG"
start_recv $JOB $NSEG
TARGETS=$(wait_targets $JOB $NSEG)
echo "targets = $TARGETS"
if [ -z "$TARGETS" ]; then echo "FAIL: A ports never published"; cat $S/t12_recv.err; kill $RECV_PID 2>/dev/null; cleanup_helpers; exit 1; fi
$PSQL -c "COPY (SELECT * FROM t12_src) TO PROGRAM
  'cbdb_lr_copyhelper send --job $JOB --seg <SEGID> --targets $TARGETS --crc' ON SEGMENT;"
echo "copy send rc=$?"
wait $RECV_PID; echo "recv psql rc=$?"; cat $S/t12_recv.err
verify A

# ---------------------------------------------------------------- phase B
JOB="t12b_$$"
NSRC=$((NSEG*2))
echo; echo "### B: $NSRC -> $NSEG (two ON SEGMENT sends, id parity split)"
$PSQL -c "TRUNCATE t12_tgt;"
start_recv $JOB $NSRC
TARGETS=$(wait_targets $JOB $NSEG)
echo "targets = $TARGETS"
if [ -z "$TARGETS" ]; then echo "FAIL: B ports never published"; cat $S/t12_recv.err; kill $RECV_PID 2>/dev/null; cleanup_helpers; exit 1; fi
$PSQL -c "COPY (SELECT * FROM t12_src WHERE id % 2 = 0) TO PROGRAM
  'cbdb_lr_copyhelper send --job $JOB --seg <SEGID> --targets $TARGETS --crc' ON SEGMENT;"
echo "copy send #1 rc=$?"
$PSQL -c "COPY (SELECT * FROM t12_src WHERE id % 2 = 1) TO PROGRAM
  'cbdb_lr_copyhelper send --job $JOB --seg <SEGID> --targets $TARGETS --crc' ON SEGMENT;"
echo "copy send #2 rc=$?"
wait $RECV_PID; echo "recv psql rc=$?"; cat $S/t12_recv.err
echo "done files:"; for d in $(seg_dirs); do cat "$d"/$JOB.*.done 2>/dev/null; done | tr '\n' ' '; echo
verify B

# ---------------------------------------------------------------- phase C
JOB="t12c_$$"
NSRC=2
echo; echo "### C: $NSRC -> $NSEG (target seg $((NSEG-1)) expects no client; sends driven by hand)"
$PSQL -c "TRUNCATE t12_tgt;"
start_recv $JOB $NSRC
TARGETS=$(wait_targets $JOB $NSRC)            # only min(nsources, M) targets publish
echo "targets = $TARGETS"
if [ -z "$TARGETS" ]; then echo "FAIL: C ports never published"; cat $S/t12_recv.err; kill $RECV_PID 2>/dev/null; cleanup_helpers; exit 1; fi
NPORTS=$($PSQL -c "SELECT count(*) FROM gp_lr_copy_ports('$JOB');")
echo "published ports = $NPORTS (expect $NSRC)"
[ "$NPORTS" = "$NSRC" ] || { echo "FAIL: C wrong number of port files"; FAILS=$((FAILS+1)); }
# emulate a 2-segment source: source seg s owns rows with id % 2 = s
SEND_PIDS=""
for s in 0 1; do
  ( $PSQL -c "COPY (SELECT * FROM t12_src WHERE id % 2 = $s) TO STDOUT" \
      | cbdb_lr_copyhelper send --job $JOB --seg $s --targets $TARGETS --crc ) &
  SEND_PIDS="$SEND_PIDS $!"
done
for p in $SEND_PIDS; do wait $p; echo "send rc=$?"; done
wait $RECV_PID; echo "recv psql rc=$?"; cat $S/t12_recv.err
echo "done files:"; for d in $(seg_dirs); do ls "$d"/$JOB.* 2>/dev/null; done | xargs -n1 basename | tr '\n' ' '; echo
verify C

echo; echo "### leftovers"
echo "helpers running: $(ps -eo comm | grep -c '^cbdb_lr_copyhel')"
echo "port files left: $(for d in $(seg_dirs); do ls "$d"/*.port 2>/dev/null; done | wc -l)"
cleanup_helpers
echo; [ $FAILS = 0 ] && echo "ALL PASS" || echo "FAILURES: $FAILS"
exit $FAILS
