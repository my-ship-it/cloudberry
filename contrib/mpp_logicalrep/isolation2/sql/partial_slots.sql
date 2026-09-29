-- A slot set that exists on some nodes and not others.
--
-- Nothing can be streamed from such a set: a distributed transaction needs
-- every node's part, so the nodes that have a slot are retaining WAL nobody
-- can reach.  There is nothing to preserve, so the synchronisation drops what
-- is there and makes the set afresh rather than stopping and asking for it to
-- be done by hand.
--
-- The state is arranged rather than waited for: the slot is made on the
-- coordinator alone, with the plain per-node function, before the
-- subscription is created.  The cluster-wide one would have made the whole
-- set, which is the point of it.
--
--   1  the subscription starts anyway, and syncs
--   2  the set is complete afterwards: every node has one

CREATE DATABASE mpp_lr_ps;

CREATE TABLE ps_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO ps_t SELECT g, 'seed' FROM generate_series(1, 200) g;
CREATE PUBLICATION ps_pub FOR TABLE ps_t;

-- the coordinator's alone: 1 of the 1 + segments the set needs
SELECT pg_create_logical_replication_slot('ps_slot', 'pgoutput', false, true) IS NOT NULL AS made_one;
SELECT count(*) AS nodes_with_a_slot FROM gp_replication_slots WHERE slot_name = 'ps_slot';

1:@db_name mpp_lr_ps: CREATE TABLE ps_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_ps: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 600 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_ps: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_ps: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION ps_sub CONNECTION %L PUBLICATION ps_pub WITH (slot_name = ''ps_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;

-- ------------------------------------------------------------------
-- 1. it starts anyway
-- ------------------------------------------------------------------
1:@db_name mpp_lr_ps: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'ps_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_ps: SELECT count(*) AS copied FROM ps_t;

INSERT INTO ps_t VALUES (901, 'after');
1:@db_name mpp_lr_ps: SELECT lr_wait('SELECT count(*) = 201 FROM ps_t') AS streamed;

-- ------------------------------------------------------------------
-- 2. and the set is whole
-- ------------------------------------------------------------------
SELECT count(*) = 1 + (SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0) AS a_slot_per_node FROM gp_replication_slots WHERE slot_name = 'ps_slot';
1:@db_name mpp_lr_ps: SELECT phase, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'ps_sub';

1:@db_name mpp_lr_ps: DROP SUBSCRIPTION ps_sub;
1:@db_name mpp_lr_ps: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('ps_slot');
DROP PUBLICATION ps_pub;
DROP TABLE ps_t;
DROP DATABASE mpp_lr_ps;
