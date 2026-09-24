-- One cut point per table, and what that means for the filter.
--
-- Each table's copy now runs under its own snapshot, taken when that copy
-- starts rather than once for the whole set: one transaction holding the
-- source's xmin for as long as the slowest copy is what this avoids.  The
-- price is that "already copied" stops being a property of a transaction and
-- becomes a property of a transaction and a table together -- the same
-- transaction can be inside one table's copy and not another's -- so the
-- filter has to be per relation, and a transaction that is covered for some
-- of its tables is applied for the rest.
--
-- Synthesised the way sync_filter does it, because catching the window for
-- real is a race: the subscription is stopped, a transaction is put exactly
-- between two snapshots, and the two are written into the two tables' rows by
-- hand.  The filter is then the only thing deciding what lands.
--
--   pc_a's cut point is taken BEFORE the transaction  -> not covered, applied
--   pc_b's cut point is taken AFTER  it               -> covered, dropped
--
-- So the row lands in pc_a and not in pc_b, from one transaction that wrote
-- both.  A whole-transaction filter cannot produce that, in either direction.

CREATE DATABASE mpp_lr_pc;

CREATE TABLE pc_a (id int, v text) DISTRIBUTED BY (id);
CREATE TABLE pc_b (id int, v text) DISTRIBUTED BY (id);
INSERT INTO pc_a SELECT g, 'seed' FROM generate_series(1, 50) g;
INSERT INTO pc_b SELECT g, 'seed' FROM generate_series(1, 50) g;
CREATE PUBLICATION pc_pub FOR TABLE pc_a, pc_b;

1:@db_name mpp_lr_pc: CREATE TABLE pc_a (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_pc: CREATE TABLE pc_b (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_pc: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_pc: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_pc: CREATE FUNCTION pc_cut() RETURNS text AS $$ SELECT 'xmax=' || xmax || ';inprogress=' || coalesce(array_to_string(in_progress, ','), '') FROM gp_current_distributed_snapshot() $$ LANGUAGE sql;

1:@db_name mpp_lr_pc: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION pc_sub CONNECTION %L PUBLICATION pc_pub WITH (slot_name = ''pc_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_pc: SELECT lr_wait($$SELECT count(*) = 2 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'pc_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;

-- stop it, so that nothing is applied until the two cut points are in place
1:@db_name mpp_lr_pc: ALTER SUBSCRIPTION pc_sub DISABLE;
1:@db_name mpp_lr_pc: SELECT lr_worker_gone() AS stopped;

-- pc_a's cut point: before the transaction below.  The marker row takes it
-- too, being the earliest of the two.
1:@db_name mpp_lr_pc: UPDATE gp_subscription_rel SET sync_snapshot = pc_cut() WHERE relid = 0 OR relid = cast('pc_a' as regclass);

-- the one transaction, writing both tables
INSERT INTO pc_a VALUES (601, 'between the two cut points');
INSERT INTO pc_b VALUES (601, 'between the two cut points');

-- pc_b's cut point: after it.  gxids are cluster-wide, so this session sees
-- what the source did even though it is connected to the other database.
1:@db_name mpp_lr_pc: UPDATE gp_subscription_rel SET sync_snapshot = pc_cut() WHERE relid = cast('pc_b' as regclass);
1:@db_name mpp_lr_pc: SELECT count(DISTINCT sync_snapshot) = 2 AS two_cut_points FROM gp_subscription_rel WHERE relid <> 0;

-- written after both, so its arrival means the stream is read through
INSERT INTO pc_a VALUES (602, 'after both cut points');
INSERT INTO pc_b VALUES (602, 'after both cut points');

1:@db_name mpp_lr_pc: ALTER SUBSCRIPTION pc_sub ENABLE;
1:@db_name mpp_lr_pc: SELECT lr_wait('SELECT count(*) = 1 FROM pc_a WHERE id = 602') AS a_caught_up;
1:@db_name mpp_lr_pc: SELECT lr_wait('SELECT count(*) = 1 FROM pc_b WHERE id = 602') AS b_caught_up;

-- the same transaction: applied for pc_a, dropped for pc_b
1:@db_name mpp_lr_pc: SELECT count(*) = 1 AS a_has_it FROM pc_a WHERE id = 601;
1:@db_name mpp_lr_pc: SELECT count(*) = 0 AS b_dropped_it FROM pc_b WHERE id = 601;
1:@db_name mpp_lr_pc: SELECT id, count(*) FROM pc_a WHERE id > 600 GROUP BY id ORDER BY id;
1:@db_name mpp_lr_pc: SELECT id, count(*) FROM pc_b WHERE id > 600 GROUP BY id ORDER BY id;
1:@db_name mpp_lr_pc: SELECT lr_wait($$SELECT phase = 'streaming' FROM gp_stat_mpp_subscription WHERE subname = 'pc_sub'$$) AS streaming;
1:@db_name mpp_lr_pc: SELECT stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'pc_sub';

1:@db_name mpp_lr_pc: DROP SUBSCRIPTION pc_sub;
1:@db_name mpp_lr_pc: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('pc_slot');
DROP PUBLICATION pc_pub;
DROP TABLE pc_a, pc_b;
DROP DATABASE mpp_lr_pc;
