-- Fault injection under a mixed workload.  A reader QE, the driven QD session
-- and the apply worker itself are each killed while the publisher is being
-- written to; afterwards the target must equal the source, with nothing lost
-- and nothing applied twice.
--
-- The kills are SIGTERM (pg_terminate_backend is all SQL has); the shell
-- script this came from also sends the worker a SIGKILL.

CREATE DATABASE mpp_lr_fa;

CREATE TABLE fa_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO fa_t SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION fa_pub FOR TABLE fa_t;

-- 60 rounds of insert / update / delete, each with a subtransaction that is
-- rolled back: +11 rows a round, and one aborted insert the target must not see
CREATE PROCEDURE fa_churn() AS $$ BEGIN FOR b IN 1001 .. 1060 LOOP INSERT INTO fa_t SELECT g, 'w' || b FROM generate_series(b * 100, b * 100 + 9) g; INSERT INTO fa_t VALUES (b * 100 + 50, 'one'); UPDATE fa_t SET v = 'u' || b WHERE id = b * 100 + 3; DELETE FROM fa_t WHERE id = b * 100 + 5; INSERT INTO fa_t VALUES (b * 100 + 60, 'sp'); BEGIN INSERT INTO fa_t VALUES (b * 100 + 61, 'rolled back'); RAISE EXCEPTION 'undo'; EXCEPTION WHEN others THEN NULL; END; UPDATE fa_t SET v = 'm' WHERE id = b * 100 + 7; COMMIT; PERFORM pg_sleep(0.5); END LOOP; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_fa: CREATE TABLE fa_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_fa: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_fa: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- The chaos: it touches nothing but pg_stat_activity, so it holds no lock on
-- the table being replicated.  Reports how many of the four kills found
-- something to kill, which is what makes the run meaningful.
1:@db_name mpp_lr_fa: CREATE FUNCTION fa_chaos() RETURNS text AS $$ DECLARE hit int DEFAULT 0; n int; BEGIN PERFORM pg_sleep(6); SELECT count(*) INTO n FROM (SELECT pg_terminate_backend(pid) FROM gp_dist_random('pg_stat_activity') WHERE gp_execution_segment() = 0 AND application_name LIKE 'mpp_apply%') x; SELECT hit + least(n, 1) INTO hit; PERFORM pg_sleep(6); SELECT count(*) INTO n FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'mpp_apply_worker' AND backend_type = 'client backend') x; SELECT hit + least(n, 1) INTO hit; PERFORM pg_sleep(6); SELECT count(*) INTO n FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database()) x; SELECT hit + least(n, 1) INTO hit; PERFORM pg_sleep(6); SELECT count(*) INTO n FROM (SELECT pg_terminate_backend(pid) FROM gp_dist_random('pg_stat_activity') WHERE gp_execution_segment() = 0 AND application_name LIKE 'mpp_apply%') x; SELECT hit + least(n, 1) INTO hit; RETURN 'kills_that_found_a_target=' || hit; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_fa: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION fa_sub CONNECTION %L PUBLICATION fa_pub WITH (slot_name = ''fa_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_fa: SELECT lr_wait('SELECT count(*) = 100 FROM fa_t') AS seeded;

-- the workload and the kills run together
2&: CALL fa_churn();
1:@db_name mpp_lr_fa: SELECT fa_chaos() AS chaos;
2<:

-- 100 seeded + 60 rounds of 11
1:@db_name mpp_lr_fa: SELECT lr_wait('SELECT count(*) = 760 FROM fa_t') AS caught_up;
-- The same statement on both sides, so the diff is the comparison.  The
-- digest is order-independent on purpose: an ordered aggregate is not
-- deterministic here, because the final gather concatenates the segments'
-- partials in whatever order they arrive.
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM fa_t;
1:@db_name mpp_lr_fa: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM fa_t;
-- the aborted inserts never reached it
1:@db_name mpp_lr_fa: SELECT count(*) AS rolled_back_rows FROM fa_t WHERE v = 'rolled back';
-- it recovered rather than giving up
1:@db_name mpp_lr_fa: SELECT phase, enabled, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'fa_sub';

1:@db_name mpp_lr_fa: DROP SUBSCRIPTION fa_sub;
1:@db_name mpp_lr_fa: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('fa_slot');
DROP PUBLICATION fa_pub;
DROP TABLE fa_t;
DROP PROCEDURE fa_churn();
DROP DATABASE mpp_lr_fa;
