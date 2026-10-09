-- The lock a unit takes on the target.
--
-- A transaction that truncates has to hold the target table exclusively while
-- it is applied; one that only inserts must not, or ordinary readers on the
-- target would be stopped by every batch.  The mode is decided per
-- transaction and not per table, so a transaction that truncates one table
-- and writes another holds both exclusively.
--
-- Between those two there is a third mode.  Without a global deadlock
-- detector -- which is the default -- the server upgrades an UPDATE or a
-- DELETE to ExclusiveLock, because a cycle of row waits spread across
-- segments is invisible to each of them and nothing would break it.  The
-- apply has to upgrade with it: it takes its locks with LOCK TABLE, which
-- gives what it is asked for and does not upgrade anything, so an apply that
-- asked for ROW EXCLUSIVE would be the one session in the cluster holding
-- cross-segment row locks under it.
--
-- What makes all of this observable is a reader on the target holding the
-- table: ROW EXCLUSIVE does not conflict with it and the apply goes through,
-- EXCLUSIVE and ACCESS EXCLUSIVE do and the apply waits until the reader is
-- done.
--
--   1  a reader does not stop inserts being applied
--   2  a reader does stop a TRUNCATE, which lands as soon as it lets go
--   3  one transaction that truncates one table and writes another waits for
--      a reader of the table it only writes
--   4  a reader does stop an UPDATE, which asks for EXCLUSIVE with no
--      detector to rely on

CREATE DATABASE mpp_lr_tl;

CREATE TABLE tl_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE tl_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE tl_b (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO tl_t SELECT g, 'seed' FROM generate_series(1, 100) g;
INSERT INTO tl_a SELECT g, 'seed' FROM generate_series(1, 50) g;
INSERT INTO tl_b SELECT g, 'seed' FROM generate_series(1, 5) g;
CREATE PUBLICATION tl_pub FOR TABLE tl_t, tl_a, tl_b;

1:@db_name mpp_lr_tl: CREATE TABLE tl_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_tl: CREATE TABLE tl_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_tl: CREATE TABLE tl_b (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_tl: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_tl: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
-- is the apply waiting for this table exclusively?  pg_locks reads the lock
-- manager itself, so a loop over it sees the present state rather than a
-- snapshot taken when the enclosing statement began
1:@db_name mpp_lr_tl: CREATE FUNCTION lr_blocked_on(t text, m text DEFAULT 'AccessExclusiveLock') RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 600 LOOP SELECT count(*) INTO n FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE c.relname = t AND l.mode = m AND NOT l.granted; IF n > 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_tl: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION tl_sub CONNECTION %L PUBLICATION tl_pub WITH (slot_name = ''tl_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_tl: SELECT lr_wait($$SELECT count(*) = 3 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'tl_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;

-- ------------------------------------------------------------------
-- 1. a reader does not stop inserts
-- ------------------------------------------------------------------
2:@db_name mpp_lr_tl: BEGIN;
2:@db_name mpp_lr_tl: SELECT count(*) AS reader_holds_it FROM tl_t;
INSERT INTO tl_t SELECT g, 'written under a reader' FROM generate_series(101, 110) g;
1:@db_name mpp_lr_tl: SELECT lr_wait('SELECT count(*) = 110 FROM tl_t') AS applied_under_a_reader;
2:@db_name mpp_lr_tl: COMMIT;

-- ------------------------------------------------------------------
-- 2. a reader does stop a TRUNCATE
-- ------------------------------------------------------------------
2:@db_name mpp_lr_tl: BEGIN;
2:@db_name mpp_lr_tl: SELECT count(*) AS reader_holds_it FROM tl_t;
TRUNCATE tl_t;
1:@db_name mpp_lr_tl: SELECT lr_blocked_on('tl_t') AS truncate_is_waiting_for_the_reader;
-- read it from the session that already holds the table: a reader arriving
-- now would queue behind the exclusive request and wait with it
2:@db_name mpp_lr_tl: SELECT count(*) AS rows_still_there FROM tl_t;
2:@db_name mpp_lr_tl: COMMIT;
1:@db_name mpp_lr_tl: SELECT lr_wait('SELECT count(*) = 0 FROM tl_t') AS truncated_once_the_reader_let_go;

-- ------------------------------------------------------------------
-- 3. the mode is the transaction's, not the table's
-- ------------------------------------------------------------------
-- the reader holds tl_b, which this transaction only writes to
2:@db_name mpp_lr_tl: BEGIN;
2:@db_name mpp_lr_tl: SELECT count(*) AS reader_holds_tl_b FROM tl_b;
BEGIN;
TRUNCATE tl_a;
INSERT INTO tl_b VALUES (101, 'in the same transaction as a truncate');
COMMIT;
1:@db_name mpp_lr_tl: SELECT lr_blocked_on('tl_b') AS waiting_for_a_table_it_only_writes;
-- and it is holding the truncated table exclusively while it waits, so the
-- truncate has not happened yet either.  Do not read that table to find out:
-- the unit holds it, and asking would mean waiting for the same thing.
1:@db_name mpp_lr_tl: SELECT count(*) > 0 AS tl_a_held_exclusively_meanwhile FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE c.relname = 'tl_a' AND l.mode = 'AccessExclusiveLock' AND l.granted;
2:@db_name mpp_lr_tl: COMMIT;
1:@db_name mpp_lr_tl: SELECT lr_wait('SELECT count(*) = 0 FROM tl_a') AS tl_a_truncated;
1:@db_name mpp_lr_tl: SELECT lr_wait('SELECT count(*) = 6 FROM tl_b') AS tl_b_written;
1:@db_name mpp_lr_tl: SELECT phase, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'tl_sub';

-- ------------------------------------------------------------------
-- 4. an UPDATE asks for EXCLUSIVE when there is no detector
-- ------------------------------------------------------------------
-- The cluster the tests run on has gp_enable_global_deadlock_detector off,
-- which is the default; with one on, this would be ROW EXCLUSIVE and the
-- holder below would not stop it.
--
-- The holder is ROW SHARE rather than the plain reader used above, because
-- that reader could not tell the two apart: ACCESS SHARE conflicts with
-- ACCESS EXCLUSIVE and nothing else, so it stops neither ROW EXCLUSIVE nor
-- EXCLUSIVE.  ROW SHARE stops EXCLUSIVE and lets ROW EXCLUSIVE through,
-- which is exactly the question.
1:@db_name mpp_lr_tl: SELECT current_setting('gp_enable_global_deadlock_detector') AS detector;
2:@db_name mpp_lr_tl: BEGIN;
2:@db_name mpp_lr_tl: LOCK TABLE tl_b IN ROW SHARE MODE;
UPDATE tl_b SET v = 'updated under a reader' WHERE id = 1;
1:@db_name mpp_lr_tl: SELECT lr_blocked_on('tl_b', 'ExclusiveLock') AS update_is_waiting_for_the_reader;
2:@db_name mpp_lr_tl: COMMIT;
1:@db_name mpp_lr_tl: SELECT lr_wait($$SELECT v = 'updated under a reader' FROM tl_b WHERE id = 1$$) AS updated_once_the_reader_let_go;

2q:
1:@db_name mpp_lr_tl: DROP SUBSCRIPTION tl_sub;
1:@db_name mpp_lr_tl: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('tl_slot');
DROP PUBLICATION tl_pub;
DROP TABLE tl_t, tl_a, tl_b;
DROP DATABASE mpp_lr_tl;
