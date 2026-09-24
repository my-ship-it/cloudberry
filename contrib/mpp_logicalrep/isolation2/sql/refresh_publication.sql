-- A table added to a running subscription is copied without stopping the
-- stream of the others, and goes live only once the stream has passed its cut
-- point (the gate in mpp_tablesync.c).
--
--   1  a base subscription on rp_a, synchronised and streaming
--   2  rp_b is added to the publication and REFRESHed in while a workload
--      writes to both: rp_a must keep being applied throughout the copy
--   3  both tables end up identical to the source, rp_b without duplicates
--      even though rows were written to it after ADD TABLE and before REFRESH
--   4  REFRESH inside a transaction block, and a table dropped from the
--      publication loses its state row
--
-- Nothing here polls rp_b while it is being copied: the copy truncates its
-- target first, and a lock taken inside a function is held until the function
-- returns, so such a poll would block the copy it waits for.

CREATE DATABASE mpp_lr_rp;

CREATE TABLE rp_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO rp_a SELECT g, 'seed' FROM generate_series(1, 2000) g;
CREATE PUBLICATION rp_pub FOR TABLE rp_a;

CREATE PROCEDURE rp_churn() AS $$ BEGIN FOR b IN 6 .. 85 LOOP INSERT INTO rp_a SELECT g, 'new' || b FROM generate_series(b * 1000, b * 1000 + 9) g; UPDATE rp_a SET v = 'upd' || b WHERE id = b * 7; DELETE FROM rp_a WHERE id = b * 11; INSERT INTO rp_b VALUES (3000000 + b, 'during' || b, b); UPDATE rp_b SET v = 'updb' || b WHERE id = b * 13; DELETE FROM rp_b WHERE id = b * 17; COMMIT; PERFORM pg_sleep(0.1); END LOOP; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_rp: CREATE TABLE rp_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_rp: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_rp: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_rp: CREATE FUNCTION lr_ready(sub text) RETURNS text AS $$ SELECT coalesce(string_agg(c.relname || '=' || cast(r.state as text), ' ' ORDER BY c.relname), 'none') FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid JOIN pg_class c ON c.oid = r.relid WHERE s.subname = $1 $$ LANGUAGE sql;

-- Watch rp_b through its state row alone, and sample rp_a -- which is not
-- being copied, so a reader there conflicts with nothing.  The window in
-- which rp_b is actually in state 'd' is far shorter than a poll interval on
-- a table this size, so what is measured is the whole of the refresh: rp_b
-- seen not yet ready, and rp_a still being applied from then until rp_b goes
-- live.
1:@db_name mpp_lr_rp: CREATE FUNCTION rp_watch() RETURNS text AS $$ DECLARE st text; first bigint; last bigint; saw bool DEFAULT false; BEGIN SELECT count(*) INTO first FROM rp_a; FOR i IN 1 .. 3000 LOOP SELECT cast(r.state as text) INTO st FROM gp_subscription_rel r JOIN pg_class c ON c.oid = r.relid WHERE c.relname = 'rp_b'; IF st IN ('i', 'd') THEN SELECT true INTO saw; END IF; SELECT count(*) INTO last FROM rp_a; EXIT WHEN st = 'r'; PERFORM pg_sleep(0.1); END LOOP; RETURN 'saw_it_not_ready=' || saw || ' a_kept_applying=' || coalesce(last > first, false); END $$ LANGUAGE plpgsql;

-- ------------------------------------------------------------------
-- 1. the base subscription
-- ------------------------------------------------------------------
1:@db_name mpp_lr_rp: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION rp_sub CONNECTION %L PUBLICATION rp_pub WITH (slot_name = ''rp_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_rp: SELECT lr_wait($$SELECT lr_ready('rp_sub') = 'rp_a=r'$$) AS base_ready;

-- ------------------------------------------------------------------
-- 2. add a second table and refresh it in, under load
-- ------------------------------------------------------------------
CREATE TABLE rp_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id);
INSERT INTO rp_b SELECT g, 'seed', g FROM generate_series(1, 400000) g;
ALTER PUBLICATION rp_pub ADD TABLE rp_b;
-- written after the publication carries rp_b but before the REFRESH: the
-- target may see these on the stream and the COPY carries them too, so only
-- the truncate on redo keeps them from being applied twice
INSERT INTO rp_b SELECT g, 'pre', g FROM generate_series(2000001, 2000050) g;
1:@db_name mpp_lr_rp: CREATE TABLE rp_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id);

2&: CALL rp_churn();
-- REFRESH is allowed in a transaction block: nothing in it connects out
1:@db_name mpp_lr_rp: BEGIN;
1:@db_name mpp_lr_rp: ALTER SUBSCRIPTION rp_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_rp: COMMIT;
1:@db_name mpp_lr_rp: SELECT rp_watch() AS during_the_copy;
2<:

-- ------------------------------------------------------------------
-- 3. both tables match the publisher
-- ------------------------------------------------------------------
1:@db_name mpp_lr_rp: SELECT lr_wait($$SELECT lr_ready('rp_sub') = 'rp_a=r rp_b=r'$$) AS both_ready;
-- 2000 seeded + 800 inserted - 80 deleted; 400000 + 50 + 80 - 80
1:@db_name mpp_lr_rp: SELECT lr_wait('SELECT count(*) = 2720 FROM rp_a') AS a_caught_up;
1:@db_name mpp_lr_rp: SELECT lr_wait('SELECT count(*) = 400050 FROM rp_b') AS b_caught_up;
-- The same statement on both sides, so the diff is the comparison.  The
-- digest is order-independent on purpose: an ordered aggregate is not
-- deterministic here, because the final gather concatenates the segments'
-- partials in whatever order they arrive.
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM rp_a;
1:@db_name mpp_lr_rp: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM rp_a;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v || '|' || w)) AS digest FROM rp_b;
1:@db_name mpp_lr_rp: SELECT count(*) AS rows, sum(hashtext(id || '|' || v || '|' || w)) AS digest FROM rp_b;
-- the rows written between ADD TABLE and REFRESH are there exactly once
1:@db_name mpp_lr_rp: SELECT count(*) AS pre_rows FROM rp_b WHERE v = 'pre';

-- ------------------------------------------------------------------
-- 4. dropping a table from the publication drops its state row
-- ------------------------------------------------------------------
ALTER PUBLICATION rp_pub DROP TABLE rp_b;
1:@db_name mpp_lr_rp: ALTER SUBSCRIPTION rp_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_rp: SELECT lr_wait($$SELECT lr_ready('rp_sub') = 'rp_a=r'$$) AS b_forgotten;
1:@db_name mpp_lr_rp: SELECT lr_ready('rp_sub') AS states_left;

1:@db_name mpp_lr_rp: DROP SUBSCRIPTION rp_sub;
1:@db_name mpp_lr_rp: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('rp_slot');
DROP PUBLICATION rp_pub;
DROP TABLE rp_a, rp_b;
DROP PROCEDURE rp_churn();
DROP DATABASE mpp_lr_rp;
