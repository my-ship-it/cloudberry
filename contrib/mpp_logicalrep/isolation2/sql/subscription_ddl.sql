-- The subscription DDL and the launcher, with a cluster to run them on.  The
-- statement contract itself is in the pg_regress test subscription_options;
-- what is here is what only a running cluster shows:
--
--   1  CREATE inside a transaction block is allowed, because nothing in it
--      connects to the publisher, and it lands in both catalogs -- the
--      server's own row disabled, the module's registry row enabled
--   2  the module's launcher starts exactly one worker, on the coordinator,
--      and none on any segment: the server's launcher must not start a stock
--      one for the same subscription
--   3  DISABLE stops that worker and ENABLE brings it back
--   4  enabled = false creates the subscription without starting anything

CREATE DATABASE mpp_lr_ddl;

CREATE TABLE ddl_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO ddl_t SELECT g, 'seed' FROM generate_series(1, 50) g;
CREATE PUBLICATION ddl_pub FOR TABLE ddl_t;
SELECT count(*) > 0 AS slots FROM gp_create_logical_replication_slot('ddl_slot', 'pgoutput', false, true);

1:@db_name mpp_lr_ddl: CREATE TABLE ddl_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_ddl: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
-- one worker per enabled registry row, once it has settled: subscription DDL
-- invalidates the syscache for every worker of the database, so each stops
-- and is started again, and only the settled count means anything
1:@db_name mpp_lr_ddl: CREATE FUNCTION ddl_settled() RETURNS bool AS $$ DECLARE want int; got int; BEGIN SELECT count(*) INTO want FROM gp_mpp_subscription WHERE enabled; FOR i IN 1 .. 400 LOOP SELECT count(*) INTO got FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF got = want THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- ------------------------------------------------------------------
-- 1. CREATE in a transaction block, and what it leaves in the two catalogs
-- ------------------------------------------------------------------
1:@db_name mpp_lr_ddl: BEGIN;
1:@db_name mpp_lr_ddl: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION ddl_sub CONNECTION %L PUBLICATION ddl_pub WITH (slot_name = ''ddl_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_ddl: COMMIT;
-- the server's row stays disabled, which is what keeps its launcher off it
1:@db_name mpp_lr_ddl: SELECT subenabled AS server_row_enabled, subslotname FROM pg_subscription WHERE subname = 'ddl_sub';
1:@db_name mpp_lr_ddl: SELECT enabled AS registry_row_enabled FROM gp_mpp_subscription WHERE subname = 'ddl_sub';

-- ------------------------------------------------------------------
-- 2. one worker, on the coordinator only
-- ------------------------------------------------------------------
1:@db_name mpp_lr_ddl: SELECT ddl_settled() AS one_worker_per_enabled_subscription;
1:@db_name mpp_lr_ddl: SELECT count(*) AS workers_here FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database();
1:@db_name mpp_lr_ddl: SELECT count(*) AS workers_on_segments FROM gp_dist_random('pg_stat_activity') WHERE backend_type = 'mpp apply worker' OR backend_type LIKE '%logical replication%worker%';
-- and it does the job
-- wait on the state row, not on the table: the copy truncates its target
-- first, and a lock taken inside a function is held until it returns
1:@db_name mpp_lr_ddl: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'ddl_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_ddl: SELECT count(*) AS copied FROM ddl_t;
INSERT INTO ddl_t SELECT g, 'more' FROM generate_series(51, 90) g;
UPDATE ddl_t SET v = 'changed' WHERE id = 7;
DELETE FROM ddl_t WHERE id = 9;
1:@db_name mpp_lr_ddl: SELECT lr_wait('SELECT count(*) = 89 FROM ddl_t') AS streamed;
1:@db_name mpp_lr_ddl: SELECT count(*) FILTER (WHERE v = 'changed') AS updated, count(*) FILTER (WHERE id = 9) AS deleted FROM ddl_t;

-- ------------------------------------------------------------------
-- 3. DISABLE stops it, ENABLE brings it back
-- ------------------------------------------------------------------
1:@db_name mpp_lr_ddl: ALTER SUBSCRIPTION ddl_sub DISABLE;
1:@db_name mpp_lr_ddl: SELECT ddl_settled() AS stopped;
1:@db_name mpp_lr_ddl: SELECT count(*) AS workers_while_disabled FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database();
1:@db_name mpp_lr_ddl: ALTER SUBSCRIPTION ddl_sub ENABLE;
1:@db_name mpp_lr_ddl: SELECT ddl_settled() AS started_again;
1:@db_name mpp_lr_ddl: SELECT count(*) AS workers_after_enable FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database();

-- ------------------------------------------------------------------
-- 4. enabled = false creates it without starting anything
-- ------------------------------------------------------------------
1:@db_name mpp_lr_ddl: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION ddl_off CONNECTION %L PUBLICATION ddl_pub WITH (slot_name = ''ddl_slot'', enabled = false)', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_ddl: SELECT enabled AS created_disabled FROM gp_mpp_subscription WHERE subname = 'ddl_off';
1:@db_name mpp_lr_ddl: SELECT ddl_settled() AS still_one_worker;
1:@db_name mpp_lr_ddl: SELECT count(*) AS workers FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database();
1:@db_name mpp_lr_ddl: DROP SUBSCRIPTION ddl_off;

1:@db_name mpp_lr_ddl: DROP SUBSCRIPTION ddl_sub;
1q:
SELECT gp_drop_replication_slot('ddl_slot');
DROP PUBLICATION ddl_pub;
DROP TABLE ddl_t;
DROP DATABASE mpp_lr_ddl;
