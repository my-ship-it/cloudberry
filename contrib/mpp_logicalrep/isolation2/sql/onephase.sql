-- The one-phase commit path, end to end.
--
-- A transaction that writes on one segment only is committed in one phase:
-- the coordinator does not prepare, so its WAL holds no distributed commit
-- record for the subscriber to order by.  What it leaves instead is a marker
-- record of its own (gp_lr_onephase_marker), written after the segment has
-- committed, and that marker is what puts the transaction in the commit order
-- of all the others.
--
-- So the thing to test is order across the two kinds: one-phase and two-phase
-- transactions touching the same row have to arrive interleaved exactly as
-- the publisher committed them.  Each step appends rather than replaces, so
-- the value is the order itself and no other order can produce it.
--
-- What the stream carries for each kind -- one event per transaction on the
-- coordinator, the gxid in the segment's own commit -- is in sender_streams.

CREATE DATABASE mpp_lr_op;

CREATE TABLE op_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO op_t SELECT g, 'seed' FROM generate_series(1, 60) g;
CREATE PUBLICATION op_pub FOR TABLE op_t;

1:@db_name mpp_lr_op: CREATE TABLE op_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_op: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_op: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION op_sub CONNECTION %L PUBLICATION op_pub WITH (slot_name = ''op_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_op: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'op_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;

-- ------------------------------------------------------------------
-- 1. one-phase alone: a chain of single-row writes
-- ------------------------------------------------------------------
-- one row is one segment, so each of these commits in one phase
UPDATE op_t SET v = v || '-p1' WHERE id = 1;
UPDATE op_t SET v = v || '-p2' WHERE id = 1;
UPDATE op_t SET v = v || '-p3' WHERE id = 1;
1:@db_name mpp_lr_op: SELECT lr_wait($$SELECT v = 'seed-p1-p2-p3' FROM op_t WHERE id = 1$$) AS chain_in_order;

-- ------------------------------------------------------------------
-- 2. one-phase and two-phase interleaved on the same row
-- ------------------------------------------------------------------
-- the unqualified updates touch every segment, so those prepare
UPDATE op_t SET v = v || '-D1';
UPDATE op_t SET v = v || '-p4' WHERE id = 1;
UPDATE op_t SET v = v || '-D2';
UPDATE op_t SET v = v || '-p5' WHERE id = 1;
UPDATE op_t SET v = v || '-D3';
1:@db_name mpp_lr_op: SELECT lr_wait($$SELECT v = 'seed-p1-p2-p3-D1-p4-D2-p5-D3' FROM op_t WHERE id = 1$$) AS interleaved_in_order;
1:@db_name mpp_lr_op: SELECT v AS the_row_that_saw_both FROM op_t WHERE id = 1;
-- every other row saw the two-phase ones only, in their own order
1:@db_name mpp_lr_op: SELECT count(*) AS rows_ending_wrong FROM op_t WHERE id <> 1 AND v <> 'seed-D1-D2-D3';

-- ------------------------------------------------------------------
-- 3. one-phase writes spread over the segments, and a read-only transaction
-- ------------------------------------------------------------------
-- a transaction that writes nothing leaves no marker and nothing to apply
BEGIN;
SELECT count(*) > 0 AS read_only_transaction FROM op_t;
COMMIT;
-- one single-row write per row: each is its own one-phase transaction, and
-- between them they land on every segment
CREATE PROCEDURE op_spread() AS $$ BEGIN FOR i IN 2 .. 60 LOOP UPDATE op_t SET v = v || '-s' WHERE id = i; COMMIT; END LOOP; END $$ LANGUAGE plpgsql;
CALL op_spread();
1:@db_name mpp_lr_op: SELECT lr_wait('SELECT count(*) = 59 FROM op_t WHERE v LIKE ''%-s''') AS all_of_them_arrived;
1:@db_name mpp_lr_op: SELECT count(DISTINCT gp_segment_id) = (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) AS they_came_from_every_segment FROM op_t WHERE v LIKE '%-s';

SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM op_t;
1:@db_name mpp_lr_op: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM op_t;
1:@db_name mpp_lr_op: SELECT phase, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'op_sub';

1:@db_name mpp_lr_op: DROP SUBSCRIPTION op_sub;
1q:
SELECT gp_drop_replication_slot('op_slot');
DROP PUBLICATION op_pub;
DROP TABLE op_t;
DROP PROCEDURE op_spread();
DROP DATABASE mpp_lr_op;
