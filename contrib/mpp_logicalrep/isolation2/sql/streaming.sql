-- Transactions the publisher cannot buffer, and transactions it abandons.
--
-- A transaction larger than logical_decoding_work_mem is sent in blocks as it
-- happens rather than whole at its commit, and the subscriber has to hold the
-- blocks back until it knows the fate of the transaction.  The setting belongs
-- to the database the walsenders connect to, so it is set there and reset at
-- the end.
--
--   1  a large transaction is streamed by every segment, and arrives whole
--   2  a large transaction with a rolled-back savepoint inside it: the
--      subscriber must drop the blocks of the part that was undone
--   3  a large transaction whose prepare is broadcast and then abandoned
--      reaches the subscriber not at all, and the stream carries on

CREATE DATABASE mpp_lr_st;

CREATE EXTENSION IF NOT EXISTS gp_inject_fault;
CREATE TABLE st_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO st_t SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION st_pub FOR TABLE st_t;
-- waiting on the publisher's own side, for its statistics to catch up
CREATE FUNCTION src_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 600 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- the readers connect after this, so they get a publisher that streams
ALTER DATABASE mpp_lr_source SET logical_decoding_work_mem = '64kB';

1:@db_name mpp_lr_st: CREATE TABLE st_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_st: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_st: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_st: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION st_sub CONNECTION %L PUBLICATION st_pub WITH (slot_name = ''st_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_st: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'st_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_st: SELECT count(*) AS copied FROM st_t;

-- ------------------------------------------------------------------
-- 1. too large to buffer
-- ------------------------------------------------------------------
INSERT INTO st_t SELECT g, repeat('x', 200) FROM generate_series(1000, 6000) g;
1:@db_name mpp_lr_st: SELECT lr_wait('SELECT count(*) = 5101 FROM st_t') AS arrived_whole;
-- and the publisher says it really did stream it, on every segment
SELECT src_wait($$SELECT count(*) = (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) FROM gp_dist_random('pg_stat_replication_slots') WHERE slot_name = 'st_slot' AND stream_txns > 0$$) AS streamed_by_every_segment;

-- ------------------------------------------------------------------
-- 2. a rolled-back savepoint inside a streamed transaction
-- ------------------------------------------------------------------
BEGIN;
INSERT INTO st_t SELECT g, 'kept' FROM generate_series(20000, 23000) g;
SAVEPOINT s;
INSERT INTO st_t SELECT g, 'dropped' FROM generate_series(30000, 33000) g;
ROLLBACK TO SAVEPOINT s;
INSERT INTO st_t VALUES (26000, 'kept after the savepoint');
COMMIT;
1:@db_name mpp_lr_st: SELECT lr_wait('SELECT count(*) = 8103 FROM st_t') AS the_kept_part_arrived;
1:@db_name mpp_lr_st: SELECT count(*) AS rows_from_a_rolled_back_savepoint FROM st_t WHERE v = 'dropped';

-- ------------------------------------------------------------------
-- 3. a large transaction whose prepare is abandoned
-- ------------------------------------------------------------------
SELECT gp_inject_fault('dtm_broadcast_prepare', 'error', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
INSERT INTO st_t SELECT g, 'abandoned' FROM generate_series(40000, 43000) g;
SELECT gp_inject_fault('dtm_broadcast_prepare', 'reset', dbid) FROM gp_segment_configuration WHERE content = -1 AND role = 'p';
-- the stream carries on, which is what proves the abandoned one was let go of
INSERT INTO st_t SELECT g, 'after the abandoned one' FROM generate_series(50000, 50009) g;
1:@db_name mpp_lr_st: SELECT lr_wait('SELECT count(*) = 8113 FROM st_t') AS streaming_again;
1:@db_name mpp_lr_st: SELECT count(*) AS rows_of_the_abandoned_transaction FROM st_t WHERE v = 'abandoned';

SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM st_t;
1:@db_name mpp_lr_st: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM st_t;
1:@db_name mpp_lr_st: SELECT phase, enabled, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'st_sub';

1:@db_name mpp_lr_st: DROP SUBSCRIPTION st_sub;
1:@db_name mpp_lr_st: SELECT lr_worker_gone() AS worker_gone;
1q:
ALTER DATABASE mpp_lr_source RESET logical_decoding_work_mem;
SELECT gp_drop_replication_slot('st_slot');
DROP PUBLICATION st_pub;
DROP TABLE st_t;
DROP FUNCTION src_wait(text);
DROP DATABASE mpp_lr_st;
