-- The initial synchronisation the module drives itself (mpp_tablesync.c):
-- slots -> barrier -> cut point -> segment-to-segment COPY -> streaming with
-- the DS_t filter.
--
--   1  automatic sync of a two-table publication under a concurrent workload
--   2  DISABLE / ENABLE: the worker comes back without re-synchronising
--   3  copy_data = false: a pre-seeded table and pre-made slots.  No COPY,
--      but the barrier and the cut point happen all the same
--   4  a sync interrupted mid-copy is redone from scratch, and DROP takes the
--      state rows with it
--
-- A wait never polls a table that is being synchronised: a lock taken inside
-- a function is held until the function returns, and the copy truncates its
-- target first, so such a poll would block the copy it waits for.

CREATE DATABASE mpp_lr_is;

CREATE TABLE is_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE is_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id);
INSERT INTO is_a SELECT g, 'seed' FROM generate_series(1, 2000) g;
INSERT INTO is_b SELECT g, 'seed', g * 2 FROM generate_series(1, 1000) g;
CREATE PUBLICATION is_pub FOR TABLE is_a, is_b;

-- a fixed workload: how it interleaves with the copy varies, what it leaves
-- behind does not, so both sides can be compared against the same digest
CREATE PROCEDURE is_churn() AS $$ BEGIN FOR b IN 6 .. 45 LOOP INSERT INTO is_a SELECT g, 'new' || b FROM generate_series(b * 1000, b * 1000 + 9) g; UPDATE is_a SET v = 'upd' || b WHERE id = b * 7; DELETE FROM is_a WHERE id = b * 11; INSERT INTO is_b VALUES (b * 1000, 'new' || b, b); UPDATE is_b SET w = w + 1 WHERE id = b * 3; COMMIT; PERFORM pg_sleep(0.1); END LOOP; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_is: CREATE TABLE is_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_is: CREATE TABLE is_b (id int PRIMARY KEY, v text, w int) DISTRIBUTED BY (id);
1:@db_name mpp_lr_is: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_is: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_is: CREATE FUNCTION lr_ready(sub text) RETURNS text AS $$ SELECT coalesce(string_agg(c.relname || '=' || cast(r.state as text), ' ' ORDER BY c.relname), 'none') FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid JOIN pg_class c ON c.oid = r.relid WHERE s.subname = $1 $$ LANGUAGE sql;

-- ------------------------------------------------------------------
-- 1. automatic initial sync while the publisher is being written to
-- ------------------------------------------------------------------
-- the workload starts first and runs across the whole of CREATE SUBSCRIPTION
2&: CALL is_churn();
1:@db_name mpp_lr_is: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION is_sub CONNECTION %L PUBLICATION is_pub WITH (slot_name = ''is_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
2<:
1:@db_name mpp_lr_is: SELECT lr_wait($$SELECT lr_ready('is_sub') = 'is_a=r is_b=r'$$) AS both_ready;
1:@db_name mpp_lr_is: SELECT lr_ready('is_sub') AS states;

-- the slots the module made for itself: the coordinator and every primary
SELECT count(*) = 1 + (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) AS slot_per_node
  FROM gp_replication_slots WHERE slot_name = 'is_slot';

-- 2000 seeded + 400 inserted - 40 deleted, and 1000 + 40, by construction
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 2360 FROM is_a') AS a_caught_up;
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 1040 FROM is_b') AS b_caught_up;
-- The same statement on both sides, so the diff is the comparison.  The
-- digest is order-independent on purpose: an ordered aggregate is not
-- deterministic here, because the final gather concatenates the segments'
-- partials in whatever order they arrive.
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_a;
1:@db_name mpp_lr_is: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_a;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v || '|' || w)) AS digest FROM is_b;
1:@db_name mpp_lr_is: SELECT count(*) AS rows, sum(hashtext(id || '|' || v || '|' || w)) AS digest FROM is_b;

-- ------------------------------------------------------------------
-- 2. DISABLE / ENABLE does not synchronise again
-- ------------------------------------------------------------------
1:@db_name mpp_lr_is: ALTER SUBSCRIPTION is_sub DISABLE;
1:@db_name mpp_lr_is: SELECT lr_wait($$SELECT count(*) = 0 FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database()$$) AS worker_gone;
INSERT INTO is_a SELECT g, 'p2' FROM generate_series(90000, 90099) g;
DELETE FROM is_b WHERE id BETWEEN 10 AND 20;
1:@db_name mpp_lr_is: ALTER SUBSCRIPTION is_sub ENABLE;
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 2460 FROM is_a') AS caught_up;
-- still the states the first synchronisation left: nothing was copied again
1:@db_name mpp_lr_is: SELECT lr_ready('is_sub') AS states_unchanged;
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 1029 FROM is_b') AS b_caught_up;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v || '|' || w)) AS digest FROM is_b;
1:@db_name mpp_lr_is: SELECT count(*) AS rows, sum(hashtext(id || '|' || v || '|' || w)) AS digest FROM is_b;

-- ------------------------------------------------------------------
-- 3. copy_data = false: nothing is copied, but the slots, the barrier and
--    the cut point are taken all the same -- they are what says where the
--    stream may be believed from, and skipping them leaves a distributed
--    transaction that was in flight when the slots were made half in the
--    stream and half not
-- ------------------------------------------------------------------
CREATE TABLE is_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO is_c SELECT g, 'seed' FROM generate_series(1, 300) g;
CREATE PUBLICATION is_pub2 FOR TABLE is_c;
-- the slots may also be the user's to make: these are reused, not remade
SELECT count(*) > 0 AS slots_made FROM gp_create_logical_replication_slot('is_slot2', 'pgoutput', false, true);
1:@db_name mpp_lr_is: CREATE TABLE is_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
-- the target's own rows say so, so a copy that should not happen is visible
1:@db_name mpp_lr_is: INSERT INTO is_c SELECT g, 'target' FROM generate_series(1, 300) g;
1:@db_name mpp_lr_is: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION is_sub2 CONNECTION %L PUBLICATION is_pub2 WITH (slot_name = ''is_slot2'', copy_data = false)', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_is: SELECT lr_wait($$SELECT lr_ready('is_sub2') = 'is_c=r'$$) AS ready;
-- the table has a state row of its own, and a cut point was recorded for it
-- and for the marker
1:@db_name mpp_lr_is: SELECT count(*) AS rows_for_sub2, count(*) FILTER (WHERE r.sync_snapshot IS NOT NULL) AS with_a_cut_point FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'is_sub2';
-- and nothing was copied: every seeded row is still the target's own
1:@db_name mpp_lr_is: SELECT count(*) FILTER (WHERE v = 'target') AS still_the_targets, count(*) FILTER (WHERE v = 'seed') AS copied_from_the_source FROM is_c;
INSERT INTO is_c SELECT g, 'p3' FROM generate_series(1001, 1050) g;
UPDATE is_c SET v = 'u3' WHERE id = 5;
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 350 FROM is_c') AS streamed;
-- id 5 was updated on both sides, so the digests match on everything the
-- stream carried; the seed rows differ by design and are excluded
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_c WHERE id > 300 OR id = 5;
1:@db_name mpp_lr_is: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_c WHERE id > 300 OR id = 5;

-- ------------------------------------------------------------------
-- 4. a sync killed mid-copy is redone, and DROP takes the state with it
-- ------------------------------------------------------------------
-- leave no other worker running, so the one terminated below is this one
1:@db_name mpp_lr_is: ALTER SUBSCRIPTION is_sub DISABLE;
1:@db_name mpp_lr_is: ALTER SUBSCRIPTION is_sub2 DISABLE;
1:@db_name mpp_lr_is: SELECT lr_wait($$SELECT count(*) = 0 FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database()$$) AS quiesced;
CREATE TABLE is_big1 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE is_big2 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO is_big1 SELECT g, md5(g::text) FROM generate_series(1, 400000) g;
INSERT INTO is_big2 SELECT g, md5(g::text) FROM generate_series(1, 400000) g;
CREATE PUBLICATION is_pub3 FOR TABLE is_big1, is_big2;
1:@db_name mpp_lr_is: CREATE TABLE is_big1 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_is: CREATE TABLE is_big2 (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_is: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION is_sub3 CONNECTION %L PUBLICATION is_pub3 WITH (slot_name = ''is_slot3'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
-- 'd' is a table being copied right now
1:@db_name mpp_lr_is: SELECT lr_wait($$SELECT count(*) > 0 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'is_sub3' AND r.state = 'd'$$) AS copying;
1:@db_name mpp_lr_is: SELECT count(*) > 0 AS worker_killed FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database()) k;
-- a row written after the interruption, which only a complete redo can carry
INSERT INTO is_big1 VALUES (2000001, 'after');
1:@db_name mpp_lr_is: SELECT lr_wait($$SELECT lr_ready('is_sub3') = 'is_big1=r is_big2=r'$$) AS redone;
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 400001 FROM is_big1') AS big1_complete;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_big1;
1:@db_name mpp_lr_is: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_big1;
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_big2;
1:@db_name mpp_lr_is: SELECT lr_wait('SELECT count(*) = 400000 FROM is_big2') AS big2_complete;
1:@db_name mpp_lr_is: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM is_big2;

-- DROP takes every state row of that subscription with it
1:@db_name mpp_lr_is: DROP SUBSCRIPTION is_sub3;
1:@db_name mpp_lr_is: SELECT (SELECT count(*) FROM gp_subscription_rel WHERE subid NOT IN (SELECT oid FROM pg_subscription)) AS orphan_rel, (SELECT count(*) FROM gp_subscription_progress WHERE subid NOT IN (SELECT oid FROM pg_subscription)) AS orphan_progress, (SELECT count(*) FROM gp_subscription_applied WHERE subid NOT IN (SELECT oid FROM pg_subscription)) AS orphan_applied;

1:@db_name mpp_lr_is: DROP SUBSCRIPTION is_sub;
1:@db_name mpp_lr_is: DROP SUBSCRIPTION is_sub2;
1:@db_name mpp_lr_is: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('is_slot');
SELECT gp_drop_replication_slot('is_slot2');
SELECT gp_drop_replication_slot('is_slot3');
DROP PUBLICATION is_pub;
DROP PUBLICATION is_pub2;
DROP PUBLICATION is_pub3;
DROP TABLE is_a, is_b, is_c, is_big1, is_big2;
DROP PROCEDURE is_churn();
DROP DATABASE mpp_lr_is;
