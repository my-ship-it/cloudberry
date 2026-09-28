-- A transaction whose segment parts take far longer to arrive than any wait
-- the coordinator would be willing to sit out.
--
-- The coordinator learns a distributed transaction committed from its own
-- stream and then waits for the segments it was dispatched to.  The wait is
-- unconditional: every one of them announces the transaction on its own
-- stream whether it wrote anything this subscription replicates or not, so
-- a participant that has not reported is behind rather than silent.
--
-- It was not always: the wait used to end after a grace period, on the
-- guess that a segment which had said nothing for long enough had nothing
-- to say.  The guess is wrong for exactly one case, and it is the expensive
-- one -- a segment still sending the transaction.  A large transaction
-- takes far longer to stream than any period that would be sensible to wait
-- by default, so giving up on it discarded every row it wrote, silently.
--
-- mpp_logicalrep.event_wait_ms is turned down to a tenth of a second here,
-- so that a transaction taking seconds to arrive is orders of magnitude
-- past what any such period would have been.
--
--   1  the initial copy
--   2  one transaction far too large to arrive quickly lands whole, not
--      partly and not at all
--   3  the stream keeps going afterwards

CREATE DATABASE mpp_lr_sa;
-- the worker reads this when it starts, so set it before the subscription
ALTER DATABASE mpp_lr_sa SET mpp_logicalrep.event_wait_ms = 100;

CREATE TABLE sa_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO sa_t SELECT g, 'seed' FROM generate_series(1, 1000) g;
CREATE PUBLICATION sa_pub FOR TABLE sa_t;

1:@db_name mpp_lr_sa: CREATE TABLE sa_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_sa: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_sa: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- ------------------------------------------------------------------
-- 1. the initial copy
-- ------------------------------------------------------------------
1:@db_name mpp_lr_sa: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION sa_sub CONNECTION %L PUBLICATION sa_pub WITH (slot_name = ''sa_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_sa: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'sa_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_sa: SELECT count(*) AS copied FROM sa_t;
1:@db_name mpp_lr_sa: SELECT current_setting('mpp_logicalrep.event_wait_ms') AS event_wait_ms;

-- ------------------------------------------------------------------
-- 2. one transaction that cannot possibly arrive within a second
-- ------------------------------------------------------------------
-- one statement, so one distributed transaction: every segment writes, and
-- the coordinator's commit event arrives while all three are still sending
INSERT INTO sa_t SELECT g, 'big ' || md5(g::text) FROM generate_series(100001, 600000) g;
SELECT count(*) AS on_the_source FROM sa_t;

-- all of it or none of it: dropped for having "written nothing", the
-- seeded thousand would be all that is here
1:@db_name mpp_lr_sa: SELECT lr_wait('SELECT count(*) = 501000 FROM sa_t') AS the_big_one_arrived;
1:@db_name mpp_lr_sa: SELECT count(*) AS rows_now, count(*) FILTER (WHERE v LIKE 'big %') AS from_the_big_one FROM sa_t;

-- ------------------------------------------------------------------
-- 3. the stream did not stop there
-- ------------------------------------------------------------------
INSERT INTO sa_t VALUES (900001, 'after');
UPDATE sa_t SET v = 'changed' WHERE id = 1;
DELETE FROM sa_t WHERE id = 2;
1:@db_name mpp_lr_sa: SELECT lr_wait($$SELECT count(*) = 501000 AND count(*) FILTER (WHERE v = 'changed') = 1 FROM sa_t$$) AS the_rest_arrived;

SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM sa_t;
1:@db_name mpp_lr_sa: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM sa_t;
1:@db_name mpp_lr_sa: SELECT phase, stuck_errors, recoveries FROM gp_stat_mpp_subscription WHERE subname = 'sa_sub';

1:@db_name mpp_lr_sa: DROP SUBSCRIPTION sa_sub;
1:@db_name mpp_lr_sa: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('sa_slot');
DROP PUBLICATION sa_pub;
DROP TABLE sa_t;
DROP DATABASE mpp_lr_sa;
