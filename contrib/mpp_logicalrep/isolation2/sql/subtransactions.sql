-- Subtransactions on the publisher.  A transaction reaches the subscriber as
-- what it committed, so what a rolled-back savepoint did must not be there --
-- including a TRUNCATE, which would otherwise empty the target.
--
--   1  a savepoint released: its rows are part of the commit
--   2  a savepoint rolled back: its rows are not, and what follows still is
--   3  nested savepoints: rolling back to the outer one drops the inner too
--   4  a TRUNCATE inside a rolled-back savepoint truncates nothing
--   5  an error caught inside the transaction (a plpgsql subtransaction)

CREATE DATABASE mpp_lr_sx;

CREATE TABLE sx_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO sx_t SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION sx_pub FOR TABLE sx_t;

1:@db_name mpp_lr_sx: CREATE TABLE sx_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_sx: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_sx: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_sx: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION sx_sub CONNECTION %L PUBLICATION sx_pub WITH (slot_name = ''sx_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_sx: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'sx_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_sx: SELECT count(*) AS copied FROM sx_t;

-- ------------------------------------------------------------------
-- 1. a savepoint that is released
-- ------------------------------------------------------------------
BEGIN;
INSERT INTO sx_t VALUES (201, 'kept');
SAVEPOINT a;
INSERT INTO sx_t VALUES (202, 'kept inside a');
RELEASE SAVEPOINT a;
INSERT INTO sx_t VALUES (203, 'kept after a');
COMMIT;
1:@db_name mpp_lr_sx: SELECT lr_wait('SELECT count(*) = 103 FROM sx_t') AS released_rows_arrived;

-- ------------------------------------------------------------------
-- 2. a savepoint that is rolled back
-- ------------------------------------------------------------------
BEGIN;
INSERT INTO sx_t VALUES (301, 'kept');
SAVEPOINT b;
INSERT INTO sx_t VALUES (302, 'dropped');
ROLLBACK TO SAVEPOINT b;
INSERT INTO sx_t VALUES (303, 'kept after rolling back to b');
COMMIT;
1:@db_name mpp_lr_sx: SELECT lr_wait('SELECT count(*) = 105 FROM sx_t') AS the_rest_arrived;
1:@db_name mpp_lr_sx: SELECT count(*) AS rows_from_a_rolled_back_savepoint FROM sx_t WHERE v = 'dropped';

-- ------------------------------------------------------------------
-- 3. nested: rolling back to the outer savepoint drops the inner one too
-- ------------------------------------------------------------------
BEGIN;
INSERT INTO sx_t VALUES (401, 'kept');
SAVEPOINT c;
INSERT INTO sx_t VALUES (402, 'dropped');
SAVEPOINT d;
INSERT INTO sx_t VALUES (403, 'dropped');
ROLLBACK TO SAVEPOINT c;
INSERT INTO sx_t VALUES (404, 'kept after rolling back to c');
COMMIT;
1:@db_name mpp_lr_sx: SELECT lr_wait('SELECT count(*) = 107 FROM sx_t') AS the_rest_arrived;
1:@db_name mpp_lr_sx: SELECT count(*) AS rows_from_a_rolled_back_savepoint FROM sx_t WHERE v = 'dropped';

-- ------------------------------------------------------------------
-- 4. a TRUNCATE that is rolled back truncates nothing
-- ------------------------------------------------------------------
BEGIN;
SAVEPOINT e;
TRUNCATE sx_t;
ROLLBACK TO SAVEPOINT e;
INSERT INTO sx_t VALUES (501, 'after an undone truncate');
COMMIT;
1:@db_name mpp_lr_sx: SELECT lr_wait('SELECT count(*) = 108 FROM sx_t') AS nothing_was_truncated;

-- ------------------------------------------------------------------
-- 5. an error caught inside the transaction
-- ------------------------------------------------------------------
DO $$ BEGIN INSERT INTO sx_t VALUES (601, 'kept by the do block'); BEGIN INSERT INTO sx_t VALUES (601, 'a duplicate key'); EXCEPTION WHEN unique_violation THEN NULL; END; END $$;
1:@db_name mpp_lr_sx: SELECT lr_wait('SELECT count(*) = 109 FROM sx_t') AS caught_error_did_not_stop_it;
1:@db_name mpp_lr_sx: SELECT count(*) AS duplicates FROM sx_t WHERE v = 'a duplicate key';

SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM sx_t;
1:@db_name mpp_lr_sx: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM sx_t;
1:@db_name mpp_lr_sx: SELECT phase, stuck_errors, recoveries FROM gp_stat_mpp_subscription WHERE subname = 'sx_sub';

1:@db_name mpp_lr_sx: DROP SUBSCRIPTION sx_sub;
1:@db_name mpp_lr_sx: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('sx_slot');
DROP PUBLICATION sx_pub;
DROP TABLE sx_t;
DROP DATABASE mpp_lr_sx;
