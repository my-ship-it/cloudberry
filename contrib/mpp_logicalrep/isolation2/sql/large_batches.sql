-- Volume: a transaction larger than a reader can hold, and a backlog larger
-- than one batch.
--
-- A reader buffers a transaction's changes until it knows the transaction's
-- fate, and spills to disk past mpp_logicalrep.reader_mem.  The driver
-- commits several transactions together, up to mpp_logicalrep.batch_max_txns
-- or batch_max_bytes or batch_max_latency_ms.  Both are turned right down
-- here so that the paths are taken by a test that stays small: the memory a
-- reader may use to 64kB, and a batch to 8 transactions.
--
--   1  the initial copy of a table with some size to it
--   2  one transaction far larger than a reader's budget, which it therefore
--      spills, arrives whole
--   3  a backlog built while the subscription is disabled is applied in many
--      batches rather than one, and exactly

CREATE DATABASE mpp_lr_lb;
-- the worker and its readers read these when they start, so set them before
-- the subscription exists
ALTER DATABASE mpp_lr_lb SET mpp_logicalrep.reader_mem = 64;
ALTER DATABASE mpp_lr_lb SET mpp_logicalrep.batch_max_txns = 8;

CREATE TABLE lb_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO lb_t SELECT g, 'seed ' || md5(g::text) FROM generate_series(1, 200000) g;
CREATE PUBLICATION lb_pub FOR TABLE lb_t;
CREATE PROCEDURE lb_backlog() AS $$ BEGIN FOR r IN 1 .. 200 LOOP INSERT INTO lb_t SELECT 1000000 + r * 100 + i, 'backlog ' || r FROM generate_series(0, 49) i; COMMIT; END LOOP; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_lb: CREATE TABLE lb_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_lb: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_lb: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- ------------------------------------------------------------------
-- 1. the initial copy
-- ------------------------------------------------------------------
1:@db_name mpp_lr_lb: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION lb_sub CONNECTION %L PUBLICATION lb_pub WITH (slot_name = ''lb_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_lb: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'lb_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_lb: SELECT count(*) AS copied FROM lb_t;
1:@db_name mpp_lr_lb: SELECT current_setting('mpp_logicalrep.reader_mem') AS reader_mem, current_setting('mpp_logicalrep.batch_max_txns') AS batch_max_txns;

-- ------------------------------------------------------------------
-- 2. one transaction a reader cannot hold in memory
-- ------------------------------------------------------------------
INSERT INTO lb_t SELECT 2000000 + g, 'one big transaction ' || md5(g::text) FROM generate_series(1, 100000) g;
1:@db_name mpp_lr_lb: SELECT lr_wait('SELECT count(*) = 300000 FROM lb_t') AS the_big_one_arrived;
1:@db_name mpp_lr_lb: SELECT count(*) AS all_of_its_rows FROM lb_t WHERE v LIKE 'one big transaction%';

-- ------------------------------------------------------------------
-- 3. a backlog, applied in batches
-- ------------------------------------------------------------------
1:@db_name mpp_lr_lb: ALTER SUBSCRIPTION lb_sub DISABLE;
1:@db_name mpp_lr_lb: SELECT lr_wait($$SELECT count(*) = 0 FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database()$$) AS stopped;
CALL lb_backlog();
SELECT count(*) AS on_the_publisher FROM lb_t;
1:@db_name mpp_lr_lb: ALTER SUBSCRIPTION lb_sub ENABLE;
1:@db_name mpp_lr_lb: SELECT lr_wait('SELECT count(*) = 310000 FROM lb_t') AS backlog_applied;
-- 200 transactions at 8 to a batch is many batches, not one.  These count
-- the worker that was started by the ENABLE above, so they are read as they
-- stand -- taking a reading before it would have read the null columns a
-- stopped subscription has.
1:@db_name mpp_lr_lb: SELECT batches > 1 AS more_than_one_batch, txns_applied >= 200 AS every_transaction_accounted_for FROM gp_stat_mpp_subscription WHERE subname = 'lb_sub';

SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM lb_t;
1:@db_name mpp_lr_lb: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM lb_t;
1:@db_name mpp_lr_lb: SELECT phase, stuck_errors, recoveries FROM gp_stat_mpp_subscription WHERE subname = 'lb_sub';

1:@db_name mpp_lr_lb: DROP SUBSCRIPTION lb_sub;
1:@db_name mpp_lr_lb: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('lb_slot');
DROP PUBLICATION lb_pub;
DROP TABLE lb_t;
DROP PROCEDURE lb_backlog();
DROP DATABASE mpp_lr_lb;
