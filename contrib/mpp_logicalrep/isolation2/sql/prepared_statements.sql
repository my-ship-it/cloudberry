-- How the publisher's DML was written does not change what is replicated.
--
--   1  PREPARE / EXECUTE, often enough that the plan cache goes generic
--   2  the same forced generic, and forced custom
--   3  a prepared statement inside a transaction, with a savepoint rolled back
--   4  an UPDATE of the distribution key, which moves a row from one segment
--      to another: on the publisher it is a delete and an insert on two
--      different segments, and the subscriber has to end up with one row
--   5  COPY, and INSERT ... SELECT

CREATE DATABASE mpp_lr_ps;

CREATE TABLE ps_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO ps_t SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION ps_pub FOR TABLE ps_t;

1:@db_name mpp_lr_ps: CREATE TABLE ps_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_ps: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_ps: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_ps: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION ps_sub CONNECTION %L PUBLICATION ps_pub WITH (slot_name = ''ps_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_ps: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'ps_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_ps: SELECT count(*) AS copied FROM ps_t;

-- ------------------------------------------------------------------
-- 1. prepared DML, executed until the plan cache has made up its mind
-- ------------------------------------------------------------------
PREPARE ps_ins(int, text) AS INSERT INTO ps_t VALUES ($1, $2);
PREPARE ps_upd(int, text) AS UPDATE ps_t SET v = $2 WHERE id = $1;
PREPARE ps_del(int) AS DELETE FROM ps_t WHERE id = $1;
EXECUTE ps_ins(201, 'prepared');
EXECUTE ps_ins(202, 'prepared');
EXECUTE ps_ins(203, 'prepared');
EXECUTE ps_ins(204, 'prepared');
EXECUTE ps_ins(205, 'prepared');
EXECUTE ps_ins(206, 'prepared');
EXECUTE ps_ins(207, 'prepared');
EXECUTE ps_upd(201, 'prepared then updated');
EXECUTE ps_del(202);
-- Not count(*) = 106 alone: the count passes through 106 on the way up, at
-- the sixth of the seven inserts, and the update and the delete have not
-- arrived then.  Wait for the end state itself.
1:@db_name mpp_lr_ps: SELECT lr_wait($$SELECT count(*) = 106 AND count(*) FILTER (WHERE v = 'prepared then updated') = 1 AND count(*) FILTER (WHERE id = 202) = 0 FROM ps_t$$) AS prepared_dml_arrived;
1:@db_name mpp_lr_ps: SELECT count(*) FILTER (WHERE v = 'prepared then updated') AS updated, count(*) FILTER (WHERE id = 202) AS deleted FROM ps_t;

-- ------------------------------------------------------------------
-- 2. the plan forced either way
-- ------------------------------------------------------------------
SET plan_cache_mode = force_generic_plan;
EXECUTE ps_ins(301, 'generic plan');
EXECUTE ps_upd(301, 'generic plan, updated');
SET plan_cache_mode = force_custom_plan;
EXECUTE ps_ins(302, 'custom plan');
SET plan_cache_mode = auto;
1:@db_name mpp_lr_ps: SELECT lr_wait('SELECT count(*) = 108 FROM ps_t') AS both_plans_arrived;
1:@db_name mpp_lr_ps: SELECT count(*) FILTER (WHERE v = 'generic plan, updated') AS generic, count(*) FILTER (WHERE v = 'custom plan') AS custom FROM ps_t;

-- ------------------------------------------------------------------
-- 3. a prepared statement in a transaction, with a savepoint rolled back
-- ------------------------------------------------------------------
BEGIN;
EXECUTE ps_ins(401, 'kept');
SAVEPOINT p;
EXECUTE ps_ins(402, 'dropped');
ROLLBACK TO SAVEPOINT p;
EXECUTE ps_ins(403, 'kept');
COMMIT;
1:@db_name mpp_lr_ps: SELECT lr_wait('SELECT count(*) = 110 FROM ps_t') AS the_rest_arrived;
1:@db_name mpp_lr_ps: SELECT count(*) AS rows_from_a_rolled_back_savepoint FROM ps_t WHERE v = 'dropped';

-- ------------------------------------------------------------------
-- 4. an UPDATE that moves a row to another segment
-- ------------------------------------------------------------------
-- not published, so its writes are in no stream: where the row was, and
-- where it went
CREATE TABLE ps_seg (step text, seg int) DISTRIBUTED BY (step);
INSERT INTO ps_seg SELECT 'before', gp_segment_id FROM ps_t WHERE id = 5;
UPDATE ps_t SET id = 500005 WHERE id = 5;
INSERT INTO ps_seg SELECT 'after', gp_segment_id FROM ps_t WHERE id = 500005;
-- which is what makes this a move and not an ordinary update: on the
-- publisher it became a delete on one segment and an insert on another
SELECT (SELECT seg FROM ps_seg WHERE step = 'before') <> (SELECT seg FROM ps_seg WHERE step = 'after') AS it_really_changed_segment;
1:@db_name mpp_lr_ps: SELECT lr_wait('SELECT count(*) = 1 FROM ps_t WHERE id = 500005') AS moved;
1:@db_name mpp_lr_ps: SELECT count(*) AS rows_now, count(*) FILTER (WHERE id = 5) AS left_behind FROM ps_t;

-- ------------------------------------------------------------------
-- 5. COPY and INSERT ... SELECT
-- ------------------------------------------------------------------
COPY ps_t FROM PROGRAM 'printf ''601\tcopied\n602\tcopied\n''';
INSERT INTO ps_t SELECT g, 'insert select' FROM generate_series(701, 720) g;
1:@db_name mpp_lr_ps: SELECT lr_wait('SELECT count(*) = 132 FROM ps_t') AS bulk_arrived;

SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM ps_t;
1:@db_name mpp_lr_ps: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM ps_t;
1:@db_name mpp_lr_ps: SELECT phase, stuck_errors, recoveries FROM gp_stat_mpp_subscription WHERE subname = 'ps_sub';

1:@db_name mpp_lr_ps: DROP SUBSCRIPTION ps_sub;
1:@db_name mpp_lr_ps: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('ps_slot');
DROP PUBLICATION ps_pub;
DROP TABLE ps_t, ps_seg;
DROP DATABASE mpp_lr_ps;
