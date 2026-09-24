-- Applying the same transaction twice.
--
-- The subscriber records how far it has got in the same transaction that
-- applies the work, so a worker that dies between the two cannot exist.  What
-- would give that away is a replay of work already applied, and a table with a
-- unique constraint would hide it behind an error: this one has none, so a
-- second copy of a transaction shows up as extra rows and as an id that
-- appears twice.
--
-- The workload is inserts only, one transaction per round, while the worker,
-- its gangs and its driven session are killed under it.  SIGTERM is what SQL
-- can send; the worker dies in the middle of a unit either way, which is the
-- part that matters.

CREATE DATABASE mpp_lr_cr;

-- no primary key and no unique index: a row applied twice stays
CREATE TABLE cr_t (id int, v text) DISTRIBUTED BY (id);
INSERT INTO cr_t SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION cr_pub FOR TABLE cr_t;
-- a waiter on the source side too, for the slot the worker leaves behind
CREATE FUNCTION lr_wait_src(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 600 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
CREATE PROCEDURE cr_churn() AS $$ BEGIN FOR r IN 1 .. 60 LOOP INSERT INTO cr_t SELECT r * 1000 + i, 'round ' || r FROM generate_series(0, 19) i; COMMIT; PERFORM pg_sleep(0.15); END LOOP; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_cr: CREATE TABLE cr_t (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_cr: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_cr: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
-- Kills the worker, its reader and writer gang QEs, and the session it drives
-- them from, round after round.  It reads pg_stat_activity only, so it holds
-- no lock on the table being applied to.
-- Six rounds of killing, and each round has to look again: pg_stat_activity
-- is a snapshot taken once per transaction, so without clearing it every
-- round would terminate the same pid the first one already did.
1:@db_name mpp_lr_cr: CREATE FUNCTION cr_chaos() RETURNS text AS $$ DECLARE hit int DEFAULT 0; n int; BEGIN FOR r IN 1 .. 6 LOOP PERFORM pg_sleep(2); PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM (SELECT pg_terminate_backend(pid) FROM gp_dist_random('pg_stat_activity') WHERE application_name LIKE 'mpp_apply%') a; SELECT hit + least(n, 1) INTO hit; SELECT count(*) INTO n FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = current_database() AND (backend_type = 'mpp apply worker' OR application_name = 'mpp_apply_worker')) b; SELECT hit + least(n, 1) INTO hit; END LOOP; RETURN 'something_was_killed=' || (hit > 0); END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_cr: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION cr_sub CONNECTION %L PUBLICATION cr_pub WITH (slot_name = ''cr_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_cr: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'cr_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_cr: SELECT count(*) AS copied FROM cr_t;

-- the workload and the killing run together
2&: CALL cr_churn();
1:@db_name mpp_lr_cr: SELECT cr_chaos() AS chaos;
2<:

-- 100 seeded + 60 rounds of 20, each id once
1:@db_name mpp_lr_cr: SELECT lr_wait('SELECT count(*) = 1300 FROM cr_t') AS caught_up;
1:@db_name mpp_lr_cr: SELECT count(*) AS rows, count(DISTINCT id) AS distinct_ids, count(*) - count(DISTINCT id) AS applied_twice FROM cr_t;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM cr_t;
1:@db_name mpp_lr_cr: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM cr_t;
-- every round is there in full: none half-applied
1:@db_name mpp_lr_cr: SELECT count(*) AS rounds_not_of_twenty FROM (SELECT v, count(*) AS n FROM cr_t WHERE v LIKE 'round %' GROUP BY v) r WHERE n <> 20;
-- the phase is waited for, not sampled: the last kill can land after the rows
-- have caught up, and the launcher takes a moment to start the worker again
1:@db_name mpp_lr_cr: SELECT lr_wait($$SELECT phase = 'streaming' FROM gp_stat_mpp_subscription WHERE subname = 'cr_sub'$$) AS streaming_again;
1:@db_name mpp_lr_cr: SELECT enabled, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'cr_sub';

1:@db_name mpp_lr_cr: DROP SUBSCRIPTION cr_sub;
1:@db_name mpp_lr_cr: SELECT lr_worker_gone() AS worker_gone;
1q:
-- the worker is gone, but the walsender it left on the source takes a moment
-- to notice and let the slot go
SELECT lr_wait_src($$SELECT count(*) = 0 FROM gp_replication_slots WHERE slot_name = 'cr_slot' AND active$$) AS slot_released;
SELECT gp_drop_replication_slot('cr_slot');
DROP FUNCTION lr_wait_src(text);
DROP PUBLICATION cr_pub;
DROP TABLE cr_t;
DROP PROCEDURE cr_churn();
DROP DATABASE mpp_lr_cr;
