-- The filter the cut point leaves behind.
--
-- What the initial copy contains is defined by the distributed snapshot it
-- ran under, DS_t = (xmax, the gxids in flight at that moment).  The
-- streaming phase then drops every transaction that snapshot already saw,
-- and applies the rest.  DS_t is kept in gp_subscription_rel.sync_snapshot
-- and read back into the worker on every start, so the filter outlives the
-- synchronisation that made it -- which is the part worth pinning.
--
-- Catching a real transaction in flight at a real cut point is a race (the
-- barrier exists to make the window small), so the cut point here is
-- synthesised: the subscription is stopped, the source is given exactly the
-- transactions that tell the two answers apart, and sync_snapshot is written
-- by hand from a snapshot taken while one of them is still open.  The filter
-- is then the only thing deciding what lands.
--
--   501  committed before the cut point                 -> the copy has it,
--                                                          must be dropped
--   502  in flight at the cut point                     -> the copy does not,
--                                                          must be applied
--   503  committed before it too                        -> dropped
--   504  committed after it                             -> applied
--
-- Both directions are needed.  That 501 and 503 are dropped shows the filter
-- was loaded at all; that 502 is applied shows the in-flight list it holds is
-- the one that was stored, rather than whatever the memory it was written in
-- happens to hold by then.

CREATE DATABASE mpp_lr_sf;

CREATE TABLE sf_t (id int, v text) DISTRIBUTED BY (id);
INSERT INTO sf_t SELECT g, 'seed' FROM generate_series(1, 50) g;
CREATE PUBLICATION sf_pub FOR TABLE sf_t;

1:@db_name mpp_lr_sf: CREATE TABLE sf_t (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_sf: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_sf: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

1:@db_name mpp_lr_sf: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION sf_sub CONNECTION %L PUBLICATION sf_pub WITH (slot_name = ''sf_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_sf: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'sf_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;

-- stop it, so that nothing is applied until the filter has been installed
1:@db_name mpp_lr_sf: ALTER SUBSCRIPTION sf_sub DISABLE;
1:@db_name mpp_lr_sf: SELECT lr_worker_gone() AS stopped;

INSERT INTO sf_t VALUES (501, 'committed before the cut point');
2: BEGIN;
2: INSERT INTO sf_t VALUES (502, 'in flight at the cut point');
-- something has to finish after it, or CreateDistributedSnapshot leaves it
-- out for being at or above xmax rather than below it and in flight
INSERT INTO sf_t VALUES (503, 'committed before it as well');

-- the cut point, as the synchronisation would have recorded it.  gxids are
-- cluster-wide, so this session sees the one the source is holding open even
-- though it is connected to the other database.
1:@db_name mpp_lr_sf: UPDATE gp_subscription_rel SET sync_snapshot = (SELECT 'xmax=' || xmax || ';inprogress=' || coalesce(array_to_string(in_progress, ','), '') FROM gp_current_distributed_snapshot()) WHERE relid = 0;
1:@db_name mpp_lr_sf: SELECT sync_snapshot LIKE '%inprogress=_%' AS it_caught_one_in_flight FROM gp_subscription_rel WHERE relid = 0;

2: COMMIT;
2q:
INSERT INTO sf_t VALUES (504, 'committed after the cut point');

1:@db_name mpp_lr_sf: ALTER SUBSCRIPTION sf_sub ENABLE;
-- 504 is past the cut point, so its arrival means the stream has been read
-- through everything above
1:@db_name mpp_lr_sf: SELECT lr_wait('SELECT count(*) = 1 FROM sf_t WHERE id = 504') AS the_stream_caught_up;

1:@db_name mpp_lr_sf: SELECT id, count(*) FROM sf_t WHERE id > 500 GROUP BY id ORDER BY id;
1:@db_name mpp_lr_sf: SELECT count(*) = 0 AS r501_dropped FROM sf_t WHERE id = 501;
1:@db_name mpp_lr_sf: SELECT count(*) = 1 AS r502_applied FROM sf_t WHERE id = 502;
1:@db_name mpp_lr_sf: SELECT count(*) = 0 AS r503_dropped FROM sf_t WHERE id = 503;
1:@db_name mpp_lr_sf: SELECT phase, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'sf_sub';

1:@db_name mpp_lr_sf: DROP SUBSCRIPTION sf_sub;
1:@db_name mpp_lr_sf: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('sf_slot');
DROP PUBLICATION sf_pub;
DROP TABLE sf_t;
DROP DATABASE mpp_lr_sf;
