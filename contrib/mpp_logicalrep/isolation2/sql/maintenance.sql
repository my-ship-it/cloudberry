-- Maintenance on the publisher, under a running subscription.
--
--   TRUNCATE is replicated: it is a message of its own in the stream, and the
--   unit that carries it takes an AccessExclusiveLock on the target
--   VACUUM, ANALYZE change nothing and are in no stream
--   VACUUM FULL and CLUSTER rewrite the heap into a new relfilenode, which is
--   a catalog change and not a row change: the target must not move, and the
--   stream must carry on afterwards
--
-- Every wait here is on a marker table rather than on the table being
-- maintained.  A lock taken inside a function is held until the function
-- returns, so polling mt_t would block an apply that wants it exclusively --
-- which is exactly what the TRUNCATE below does.  The marker is written in a
-- transaction of its own, so commit order is what makes the wait sound: when
-- the marker has arrived, everything before it has been applied.

CREATE DATABASE mpp_lr_mt;

CREATE TABLE mt_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE mt_mark (id int PRIMARY KEY) DISTRIBUTED BY (id);
INSERT INTO mt_t SELECT g, 'seed' FROM generate_series(1, 500) g;
CREATE PUBLICATION mt_pub FOR TABLE mt_t, mt_mark;
-- not published, so it is in no stream: where the filenodes are remembered
CREATE TABLE mt_fn (step text, fn bigint) DISTRIBUTED BY (step);

1:@db_name mpp_lr_mt: CREATE TABLE mt_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_mt: CREATE TABLE mt_mark (id int PRIMARY KEY) DISTRIBUTED BY (id);
1:@db_name mpp_lr_mt: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_mt: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION mt_sub CONNECTION %L PUBLICATION mt_pub WITH (slot_name = ''mt_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_mt: SELECT lr_wait($$SELECT count(*) = 2 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'mt_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_mt: SELECT count(*) AS copied FROM mt_t;

-- ------------------------------------------------------------------
-- VACUUM and ANALYZE: nothing to replicate
-- ------------------------------------------------------------------
VACUUM mt_t;
ANALYZE mt_t;
INSERT INTO mt_mark VALUES (1);
1:@db_name mpp_lr_mt: SELECT lr_wait('SELECT count(*) = 1 FROM mt_mark') AS caught_up;
1:@db_name mpp_lr_mt: SELECT count(*) AS rows_still_there FROM mt_t;
1:@db_name mpp_lr_mt: SELECT phase, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'mt_sub';

-- ------------------------------------------------------------------
-- VACUUM FULL: the heap is rewritten under the subscription
-- ------------------------------------------------------------------
INSERT INTO mt_fn SELECT 'before', sum(cast(relfilenode as bigint)) FROM gp_dist_random('pg_class') WHERE relname = 'mt_t';
VACUUM FULL mt_t;
INSERT INTO mt_fn SELECT 'after', sum(cast(relfilenode as bigint)) FROM gp_dist_random('pg_class') WHERE relname = 'mt_t';
SELECT (SELECT fn FROM mt_fn WHERE step = 'before') <> (SELECT fn FROM mt_fn WHERE step = 'after') AS heap_was_rewritten;
INSERT INTO mt_mark VALUES (2);
1:@db_name mpp_lr_mt: SELECT lr_wait('SELECT count(*) = 2 FROM mt_mark') AS caught_up;
1:@db_name mpp_lr_mt: SELECT count(*) AS unmoved FROM mt_t;
-- and the stream carries on over the new relfilenode
INSERT INTO mt_t SELECT g, 'after vacuum full' FROM generate_series(501, 560) g;
1:@db_name mpp_lr_mt: SELECT lr_wait('SELECT count(*) = 560 FROM mt_t') AS still_streaming;

-- ------------------------------------------------------------------
-- CLUSTER: the other rewrite
-- ------------------------------------------------------------------
DELETE FROM mt_fn;
INSERT INTO mt_fn SELECT 'before', sum(cast(relfilenode as bigint)) FROM gp_dist_random('pg_class') WHERE relname = 'mt_t';
CLUSTER mt_t USING mt_t_pkey;
INSERT INTO mt_fn SELECT 'after', sum(cast(relfilenode as bigint)) FROM gp_dist_random('pg_class') WHERE relname = 'mt_t';
SELECT (SELECT fn FROM mt_fn WHERE step = 'before') <> (SELECT fn FROM mt_fn WHERE step = 'after') AS heap_was_rewritten;
INSERT INTO mt_t SELECT g, 'after cluster' FROM generate_series(561, 570) g;
1:@db_name mpp_lr_mt: SELECT lr_wait('SELECT count(*) = 570 FROM mt_t') AS still_streaming;

-- ------------------------------------------------------------------
-- TRUNCATE: replicated, and the stream goes on afterwards
-- ------------------------------------------------------------------
TRUNCATE mt_t;
INSERT INTO mt_mark VALUES (3);
1:@db_name mpp_lr_mt: SELECT lr_wait('SELECT count(*) = 3 FROM mt_mark') AS caught_up;
1:@db_name mpp_lr_mt: SELECT count(*) AS emptied FROM mt_t;
INSERT INTO mt_t SELECT g, 'after truncate' FROM generate_series(1, 40) g;
1:@db_name mpp_lr_mt: SELECT lr_wait('SELECT count(*) = 40 FROM mt_t') AS streaming_again;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM mt_t;
1:@db_name mpp_lr_mt: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM mt_t;
1:@db_name mpp_lr_mt: SELECT phase, enabled, stuck_errors, recoveries FROM gp_stat_mpp_subscription WHERE subname = 'mt_sub';

1:@db_name mpp_lr_mt: DROP SUBSCRIPTION mt_sub;
1q:
SELECT gp_drop_replication_slot('mt_slot');
DROP PUBLICATION mt_pub;
DROP TABLE mt_t, mt_mark, mt_fn;
DROP DATABASE mpp_lr_mt;
