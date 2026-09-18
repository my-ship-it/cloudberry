-- What the publisher's WAL decodes to: the two streams an MPP apply driver
-- reads, seen through test_decoding rather than through this module.
--
-- The coordinator stream carries one event per distributed transaction and
-- none of the rows; each segment stream carries that segment's rows and the
-- gxid that ties them to the coordinator's event.  What is asserted is the
-- relation between the two:
--
--   1  every distributed transaction announced exactly once on the
--      coordinator, whether it committed in two phases or in one
--   2  the gxids the segments carry are the ones the coordinator announced,
--      and the announced participant list is exactly the set of segments
--      whose stream carried it
--   3  a transaction that rolled back reaches neither stream
--   4  a write with no distributed transaction (utility mode) reaches its own
--      segment's stream alone, under a commit that carries no gxid
--   5  a transaction whose prepare is broadcast and then abandoned ends in
--      ROLLBACK PREPARED on the segments and is never announced
--   6  TRUNCATE is a message of its own, and a rolled-back savepoint is in
--      no stream at all
--   7  a transaction too large for logical_decoding_work_mem is streamed
--
-- This is the sender, which is kernel code and owes nothing to this module.
-- It lives in this suite because this is the one that is run against a
-- cluster with wal_level = logical.  It also wants the fault injector, which
-- is how a prepare is abandoned on purpose.
--
-- Three primaries, as the rest of isolation2 assumes: the per-segment
-- readings below are addressed by content id.
SELECT count(*) = 3 AS three_primaries
  FROM gp_segment_configuration WHERE role = 'p' AND content >= 0;

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;

CREATE TABLE s7_t (id int, v text) DISTRIBUTED BY (id);
-- Where each reading is put down, so that one query can compare them: a
-- utility-mode insert stays on the segment that made it and an ordinary query
-- reads them all.  Unlogged, and every reading skips empty transactions, so
-- that writing down a stream does not appear in the next reading of it.
CREATE UNLOGGED TABLE s7_raw (node int, data text) DISTRIBUTED BY (node);

-- what the coordinator announced, and what the segments actually carried
CREATE VIEW s7_announced AS
  SELECT cast(substring(data from 'gxid ([0-9]+)') as bigint) AS gxid,
         cast(seg as int) AS node
    FROM s7_raw, unnest(string_to_array(substring(data from 'segs (.*)$'), ' ')) AS seg
   WHERE node = -1 AND data LIKE 'DISTRIBUTED COMMIT%' AND seg ~ '^[0-9]+$';
CREATE VIEW s7_carried AS
  SELECT DISTINCT
         cast(coalesce(substring(data from 'PREPARE TRANSACTION ''([0-9]+)'''),
                       substring(data from 'gxid ([0-9]+)')) as bigint) AS gxid,
         node
    FROM s7_raw
   WHERE node >= 0
     AND (data LIKE 'PREPARE TRANSACTION%' OR data LIKE 'COMMIT%(gxid %');

SELECT count(*) = 1 + (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) AS a_slot_per_node
  FROM gp_create_logical_replication_slot('s7_slot', 'test_decoding', false, true);

-- ------------------------------------------------------------------
-- 1. two-phase, one-phase, and one that rolls back
-- ------------------------------------------------------------------
-- written on every segment, so the coordinator prepares
INSERT INTO s7_t SELECT g, 'twophase' FROM generate_series(1, 30) g;
-- one row is one participant: committed in one phase, and all the coordinator
-- leaves behind is its marker record
INSERT INTO s7_t VALUES (1, 'onephase');
-- and one that no stream may carry
BEGIN;
INSERT INTO s7_t VALUES (2, 'aborted');
ROLLBACK;

INSERT INTO s7_raw SELECT -1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
0U: INSERT INTO s7_raw SELECT 0, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
1U: INSERT INTO s7_raw SELECT 1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
2U: INSERT INTO s7_raw SELECT 2, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');

-- the coordinator carries the events and none of the rows
SELECT count(*) FILTER (WHERE data LIKE 'DISTRIBUTED COMMIT%') AS events,
       count(DISTINCT substring(data from 'gxid ([0-9]+)')) FILTER (WHERE data LIKE 'DISTRIBUTED COMMIT%') AS distinct_gxids,
       count(*) FILTER (WHERE data LIKE 'table %') AS rows_on_the_coordinator
  FROM s7_raw WHERE node = -1;

-- the two-phase one prepared under a gid that is its gxid, on every segment it
-- wrote; the one-phase one committed on its one segment with the gxid in the
-- commit itself
SELECT count(*) FILTER (WHERE data LIKE 'PREPARE TRANSACTION%') AS prepares,
       count(*) FILTER (WHERE data LIKE 'COMMIT PREPARED%') AS commit_prepareds,
       count(*) FILTER (WHERE data LIKE 'COMMIT%(gxid %') AS commits_with_a_gxid
  FROM s7_raw WHERE node >= 0;

SELECT (SELECT count(*) FROM (SELECT * FROM s7_announced EXCEPT SELECT * FROM s7_carried) a) AS announced_but_not_carried,
       (SELECT count(*) FROM (SELECT * FROM s7_carried EXCEPT SELECT * FROM s7_announced) b) AS carried_but_not_announced;

-- and the one that rolled back is in no stream at all
SELECT count(*) AS lines_mentioning_the_aborted_row FROM s7_raw WHERE data LIKE '%aborted%';

-- ------------------------------------------------------------------
-- 2. a write with no distributed transaction behind it
-- ------------------------------------------------------------------
-- DELETE, not TRUNCATE: truncating is a catalog change, which is a
-- distributed transaction of its own and would be in the streams being read
DELETE FROM s7_raw;
0U: INSERT INTO s7_t VALUES (777, 'utility');

INSERT INTO s7_raw SELECT -1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
0U: INSERT INTO s7_raw SELECT 0, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
1U: INSERT INTO s7_raw SELECT 1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
2U: INSERT INTO s7_raw SELECT 2, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');

-- nothing on the coordinator: there was no distributed transaction to announce
SELECT count(*) AS coordinator_events FROM s7_raw WHERE node = -1;
-- the row and its gxid-less commit, on the segment that wrote it and nowhere else
SELECT node,
       count(*) FILTER (WHERE data LIKE '%utility%') AS the_row,
       count(*) FILTER (WHERE data LIKE 'COMMIT%' AND data NOT LIKE '%gxid%') AS commits_without_a_gxid,
       count(*) FILTER (WHERE data LIKE '%gxid%') AS anything_with_a_gxid
  FROM s7_raw WHERE node >= 0 GROUP BY node ORDER BY node;

-- ------------------------------------------------------------------
-- 3. a prepare that is broadcast and then abandoned
-- ------------------------------------------------------------------
-- DELETE, not TRUNCATE: truncating is a catalog change, which is a
-- distributed transaction of its own and would be in the streams being read
DELETE FROM s7_raw;
SELECT gp_inject_fault('dtm_broadcast_prepare', 'error', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
INSERT INTO s7_t SELECT g, 'prepared then abandoned' FROM generate_series(1, 30) g;
SELECT gp_inject_fault('dtm_broadcast_prepare', 'reset', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';

INSERT INTO s7_raw SELECT -1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
0U: INSERT INTO s7_raw SELECT 0, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
1U: INSERT INTO s7_raw SELECT 1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
2U: INSERT INTO s7_raw SELECT 2, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');

-- The segments rolled their prepared transaction back and none of it was
-- announced.  There is no PREPARE to see: decoding only reached the prepare
-- record after the transaction had already been rolled back, and PostgreSQL
-- does not hand a subscriber a prepare whose fate it already knows.
SELECT count(*) FILTER (WHERE node >= 0 AND data LIKE 'ROLLBACK PREPARED%') > 0 AS rolled_back_on_the_segments,
       count(*) FILTER (WHERE node >= 0 AND data LIKE 'COMMIT PREPARED%') AS committed,
       count(*) FILTER (WHERE node = -1) AS announced,
       count(*) FILTER (WHERE data LIKE '%prepared then abandoned%') AS rows_in_either_stream
  FROM s7_raw;
-- the rows are gone from the table too
SELECT count(*) AS rows_left FROM s7_t WHERE v = 'prepared then abandoned';

-- ------------------------------------------------------------------
-- 4. TRUNCATE, and a savepoint that is rolled back
-- ------------------------------------------------------------------
DELETE FROM s7_raw;
BEGIN;
INSERT INTO s7_t VALUES (900, 'kept');
SAVEPOINT s;
INSERT INTO s7_t VALUES (901, 'dropped by the savepoint');
ROLLBACK TO SAVEPOINT s;
COMMIT;
TRUNCATE s7_t;

INSERT INTO s7_raw SELECT -1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
0U: INSERT INTO s7_raw SELECT 0, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
1U: INSERT INTO s7_raw SELECT 1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');
2U: INSERT INTO s7_raw SELECT 2, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on');

-- the truncate is a message of its own, and what the savepoint undid is not
-- in the stream at all: a transaction reaches a subscriber as what it
-- committed
SELECT count(*) FILTER (WHERE data LIKE '%TRUNCATE%') > 0 AS truncate_is_in_the_stream,
       count(*) FILTER (WHERE data LIKE '%kept%') AS the_row_that_stayed,
       count(*) FILTER (WHERE data LIKE '%dropped by the savepoint%') AS rows_from_a_rolled_back_savepoint
  FROM s7_raw WHERE node >= 0;
-- both transactions announced, once each
SELECT count(*) AS announced FROM s7_raw WHERE node = -1 AND data LIKE 'DISTRIBUTED COMMIT%';

-- ------------------------------------------------------------------
-- 5. a transaction too large to buffer is streamed
-- ------------------------------------------------------------------
-- DELETE, not TRUNCATE: truncating is a catalog change, which is a
-- distributed transaction of its own and would be in the streams being read
DELETE FROM s7_raw;
INSERT INTO s7_t SELECT g, repeat('x', 200) FROM generate_series(1000, 9000) g;

1U: SET logical_decoding_work_mem = '64kB';
1U: INSERT INTO s7_raw SELECT 1, data FROM pg_logical_slot_get_changes('s7_slot', NULL, NULL, 'include-gxid', 'on', 'skip-empty-xacts', 'on', 'stream-changes', 'on');

SELECT count(*) FILTER (WHERE data LIKE 'opening a streamed block%') > 0 AS streamed_in_blocks,
       count(*) FILTER (WHERE data LIKE 'preparing streamed transaction%') AS streamed_prepares,
       count(*) FILTER (WHERE data LIKE 'streaming change for TXN%') = (SELECT count(*) FROM gp_dist_random('s7_t') WHERE gp_execution_segment() = 1 AND v = repeat('x', 200)) AS every_row_of_that_segment
  FROM s7_raw WHERE node = 1;

0Uq:
1Uq:
2Uq:
DROP VIEW s7_announced, s7_carried;
SELECT gp_drop_replication_slot('s7_slot');
DROP TABLE s7_t, s7_raw;
