-- The readers stop consuming the source when they are holding too much.
--
-- Changes to a table whose copy is still running are withheld by the
-- readers, and the transactions that carry them are held -- their rows stay
-- in the reader's buffer, and in its spill files past
-- mpp_logicalrep.reader_mem, until the copy finishes and the catch-up
-- replays them.  How much that is depends on how long the copy takes and how
-- fast the source is written, and nothing used to look at it: the ceiling
-- was the segment's disk.
--
-- mpp_logicalrep.hold_max is that ceiling.  Past it the coordinator asks the
-- readers to release, report and reply as usual but to read nothing, which
-- fills the socket, closes the window on the source and leaves the
-- transactions in the source's WAL -- where max_slot_wal_keep_size bounds
-- them and a lost slot is something this module recovers from.
--
--   1  a write to a table that is being added, big enough to go over a
--      hold_max turned right down
--   2  while it is held the source can be written and the readers do not
--      read it: their stream positions do not move
--   3  the copy finishes anyway -- it does not go through the readers -- and
--      the catch-up releases what was held, which lets them resume and the
--      subscription converge, with every row exactly once
--
-- The window is made rather than raced: a lock held on the source's bp_b
-- blocks the copy of it, and the job does not finish while any of its tables
-- is unfinished, so bp_c stays deferred for as long as the lock is held.
-- Writes go to bp_c, which nothing has locked.
--
-- What counts towards the limit is what a reader could be told to let go of:
-- completed transactions the driver has not released.  A transaction still
-- arriving does not count, or one larger than the limit would stop the very
-- reading its own COMMIT is waiting for.
--
-- Note that the readers are not held back while a synchronisation is still
-- waiting for the stream to reach its cut point: the tables cannot go live
-- until the applied position has passed it, and the only way to pass it is
-- to read.  Here the subscription is streaming and caught up when the cut
-- point is taken, so that wait is over almost at once.

CREATE DATABASE mpp_lr_bp;
-- the worker reads this when it starts, so set it before the subscription
ALTER DATABASE mpp_lr_bp SET mpp_logicalrep.hold_max = 64;

CREATE TABLE bp_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO bp_a SELECT g, 'seed' FROM generate_series(1, 1000) g;
CREATE TABLE bp_b (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO bp_b SELECT g, 'seed' FROM generate_series(1, 1000) g;
CREATE TABLE bp_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO bp_c SELECT g, 'seed' FROM generate_series(1, 1000) g;
CREATE PUBLICATION bp_pub FOR TABLE bp_a;

1:@db_name mpp_lr_bp: CREATE TABLE bp_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_bp: CREATE TABLE bp_b (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_bp: CREATE TABLE bp_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_bp: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_bp: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
-- reads the state rows and the statistics only: a reader on a table being
-- copied would block the copy it is waiting for
1:@db_name mpp_lr_bp: CREATE FUNCTION lr_state(rel text) RETURNS text AS $$ SELECT coalesce(min(cast(r.state as text)), 'none') FROM gp_subscription_rel r JOIN pg_class c ON c.oid = r.relid WHERE c.relname = $1 $$ LANGUAGE sql;
1:@db_name mpp_lr_bp: CREATE FUNCTION lr_held() RETURNS bigint AS $$ SELECT coalesce(max(buffered_bytes), 0) FROM gp_stat_mpp_subscription_segments WHERE subname = 'bp_sub' $$ LANGUAGE sql;
1:@db_name mpp_lr_bp: CREATE FUNCTION lr_pos() RETURNS text AS $$ SELECT coalesce(cast(max(pos) as text), 'none') FROM gp_stat_mpp_subscription_segments WHERE subname = 'bp_sub' $$ LANGUAGE sql;

1:@db_name mpp_lr_bp: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION bp_sub CONNECTION %L PUBLICATION bp_pub WITH (slot_name = ''bp_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_bp: SELECT lr_wait($$SELECT lr_state('bp_a') = 'r'$$) AS base_ready;
1:@db_name mpp_lr_bp: SELECT current_setting('mpp_logicalrep.hold_max') AS hold_max;

-- ------------------------------------------------------------------
-- 1. a write to a table that is being added
-- ------------------------------------------------------------------
-- REFRESH asks the publisher which tables it has, and that reads them, so it
-- has to happen before the lock below -- which means the subscription has to
-- be stopped over it, or the job would start before the lock was taken
1:@db_name mpp_lr_bp: ALTER SUBSCRIPTION bp_sub DISABLE;
1:@db_name mpp_lr_bp: SELECT lr_wait($$SELECT count(*) = 0 FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database()$$) AS stopped;
ALTER PUBLICATION bp_pub ADD TABLE bp_b, bp_c;
1:@db_name mpp_lr_bp: ALTER SUBSCRIPTION bp_sub REFRESH PUBLICATION;

-- the copy of bp_b reads it, so this blocks the job for as long as it is held
2: BEGIN;
2: LOCK TABLE bp_b IN ACCESS EXCLUSIVE MODE;

1:@db_name mpp_lr_bp: ALTER SUBSCRIPTION bp_sub ENABLE;
-- the job exists, so its cut point is taken and both tables are deferred
1:@db_name mpp_lr_bp: SELECT lr_wait($$SELECT phase = 'table sync' FROM gp_stat_mpp_subscription WHERE subname = 'bp_sub'$$) AS job_started;

INSERT INTO bp_c SELECT 2000000 + g, 'held ' || md5(g::text) FROM generate_series(1, 20000) g;
1:@db_name mpp_lr_bp: SELECT lr_wait('SELECT lr_held() > 65536') AS over_hold_max;
1:@db_name mpp_lr_bp: SELECT lr_state('bp_c') <> 'r' AS still_syncing;
1:@db_name mpp_lr_bp: SELECT lr_wait($$SELECT deferred_txns > 0 FROM gp_stat_mpp_subscription WHERE subname = 'bp_sub'$$) AS transactions_held;

-- ------------------------------------------------------------------
-- 2. the source is written and the readers do not read it
-- ------------------------------------------------------------------
1:@db_name mpp_lr_bp: CREATE TEMP TABLE bp_mark AS SELECT lr_pos() AS pos;
INSERT INTO bp_c SELECT 3000000 + g, 'ignored ' || md5(g::text) FROM generate_series(1, 20000) g;
SELECT count(*) AS written_while_held FROM bp_c WHERE v LIKE 'ignored%';
1:@db_name mpp_lr_bp: SELECT pg_sleep(3);
1:@db_name mpp_lr_bp: SELECT (SELECT pos FROM bp_mark) = lr_pos() AS the_readers_stopped;

-- ------------------------------------------------------------------
-- 3. the copy finishes anyway and the catch-up lets them resume
-- ------------------------------------------------------------------
2: COMMIT;
2q:
1:@db_name mpp_lr_bp: SELECT lr_wait($$SELECT lr_state('bp_c') = 'r' AND lr_state('bp_b') = 'r'$$) AS both_ready;
1:@db_name mpp_lr_bp: SELECT lr_wait('SELECT count(*) = 41000 FROM bp_c') AS c_caught_up;
1:@db_name mpp_lr_bp: SELECT count(*) AS held_rows FROM bp_c WHERE v LIKE 'held%';
1:@db_name mpp_lr_bp: SELECT count(*) AS ignored_rows FROM bp_c WHERE v LIKE 'ignored%';
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM bp_c;
1:@db_name mpp_lr_bp: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM bp_c;
1:@db_name mpp_lr_bp: SELECT lr_wait('SELECT lr_held() < 65536') AS released;
1:@db_name mpp_lr_bp: SELECT lr_wait($$SELECT phase = 'streaming' FROM gp_stat_mpp_subscription WHERE subname = 'bp_sub'$$) AS streaming_again;
1:@db_name mpp_lr_bp: SELECT stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'bp_sub';

1:@db_name mpp_lr_bp: DROP SUBSCRIPTION bp_sub;
1:@db_name mpp_lr_bp: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('bp_slot');
DROP PUBLICATION bp_pub;
DROP TABLE bp_a, bp_b, bp_c;
DROP DATABASE mpp_lr_bp;
