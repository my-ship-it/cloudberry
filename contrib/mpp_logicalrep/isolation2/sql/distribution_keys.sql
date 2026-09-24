-- What a table has to look like on both sides.
--
-- A change is applied to one row on one segment.  For that to be the right
-- row on the right segment, every time, three things have to hold, and they
-- are checked when the table joins the subscription rather than left to go
-- wrong later:
--
--   the target's distribution key is part of the source's, so a change that
--   leaves the source's key alone cannot move the row on the target -- and a
--   change of the source's key never arrives as an UPDATE, because moving a
--   row between source segments is a DELETE and an INSERT there
--
--   it is also part of what the source sends as the old row, or the segment
--   the row is leaving cannot be worked out
--
--   and one replica identity names one target row, or an UPDATE is applied
--   to whichever matching row is found first and the two sides drift apart
--   without an error
--
--   neither side is DISTRIBUTED RANDOMLY, which has no key at all
--
-- A source relation with no replica identity is exempt: it can only publish
-- INSERTs, and an INSERT is routed by the new row alone.

-- start_ignore
DROP TABLE IF EXISTS dk_same, dk_narrow, dk_full, dk_insertonly, dk_switch, dk_randsrc, dk_randtgt, dk_nouniq CASCADE;
-- end_ignore
CREATE DATABASE mpp_lr_dk;

-- accepted
CREATE TABLE dk_same (a int, b int, v text, PRIMARY KEY (a, b)) DISTRIBUTED BY (a, b);
CREATE TABLE dk_narrow (a int, b int, v text, PRIMARY KEY (a, b)) DISTRIBUTED BY (a, b);
CREATE TABLE dk_full (id int, v text) DISTRIBUTED BY (id);
CREATE TABLE dk_insertonly (id int, v text) DISTRIBUTED BY (id);
-- refused
CREATE TABLE dk_switch (v1 int, v2 int, v text, PRIMARY KEY (v1, v2)) DISTRIBUTED BY (v1);
CREATE TABLE dk_randsrc (id int, v text) DISTRIBUTED RANDOMLY;
CREATE TABLE dk_randtgt (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE dk_nouniq (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
ALTER TABLE dk_full REPLICA IDENTITY FULL;
ALTER TABLE dk_randsrc REPLICA IDENTITY FULL;

INSERT INTO dk_same VALUES (1, 1, 'x');
INSERT INTO dk_narrow VALUES (1, 1, 'x');
INSERT INTO dk_full VALUES (1, 'x');
INSERT INTO dk_insertonly VALUES (1, 'x');
INSERT INTO dk_switch VALUES (1, 1, 'x');
INSERT INTO dk_randsrc VALUES (1, 'x');
INSERT INTO dk_randtgt VALUES (1, 'x');
INSERT INTO dk_nouniq VALUES (1, 'x');

CREATE PUBLICATION dk_p_same FOR TABLE dk_same;
CREATE PUBLICATION dk_p_narrow FOR TABLE dk_narrow;
CREATE PUBLICATION dk_p_full FOR TABLE dk_full;
CREATE PUBLICATION dk_p_insertonly FOR TABLE dk_insertonly;
CREATE PUBLICATION dk_p_switch FOR TABLE dk_switch;
CREATE PUBLICATION dk_p_randsrc FOR TABLE dk_randsrc;
CREATE PUBLICATION dk_p_randtgt FOR TABLE dk_randtgt;
CREATE PUBLICATION dk_p_nouniq FOR TABLE dk_nouniq;

-- The target: same, narrowed, a primary key against a FULL source, no
-- identity at all, then the four that cannot work.  Each subscription is
-- made and judged before the next one starts: they create replication slots
-- on every source node, and eight of them at once race each other.
1:@db_name mpp_lr_dk: CREATE TABLE dk_same (a int, b int, v text, PRIMARY KEY (a, b)) DISTRIBUTED BY (a, b);
1:@db_name mpp_lr_dk: CREATE TABLE dk_narrow (a int, b int, v text, PRIMARY KEY (a, b)) DISTRIBUTED BY (a);
1:@db_name mpp_lr_dk: CREATE TABLE dk_full (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_dk: CREATE TABLE dk_insertonly (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_dk: CREATE TABLE dk_switch (v1 int, v2 int, v text, PRIMARY KEY (v1, v2)) DISTRIBUTED BY (v2);
1:@db_name mpp_lr_dk: CREATE TABLE dk_randsrc (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_dk: CREATE TABLE dk_randtgt (id int, v text) DISTRIBUTED RANDOMLY;
1:@db_name mpp_lr_dk: CREATE TABLE dk_nouniq (id int, v text) DISTRIBUTED BY (id);

1:@db_name mpp_lr_dk: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_dk: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_dk: CREATE FUNCTION dk_sub(t text) RETURNS void AS $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION dk_s_%s CONNECTION %L PUBLICATION dk_p_%s WITH (slot_name = ''dk_sl_%s'')', t, 'dbname=mpp_lr_source port=' || current_setting('port'), t, t); END $$ LANGUAGE plpgsql;
-- what became of each one: the row arrived, or it stopped and said why
1:@db_name mpp_lr_dk: CREATE FUNCTION dk_verdict(t text) RETURNS text AS $$ DECLARE en bool; why text; n bigint; BEGIN FOR i IN 1 .. 1800 LOOP SELECT enabled, disable_reason INTO en, why FROM gp_mpp_subscription WHERE subname = 'dk_s_' || t; IF NOT en THEN RETURN 'refused - ' || coalesce(substring(why from 'logical replication[^"]*'), left(coalesce(why, 'no reason recorded'), 80)); END IF; EXECUTE format('SELECT count(*) FROM %I', 'dk_' || t) INTO n; IF n = 1 THEN RETURN 'replicated'; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN 'neither, after 180 s'; END $$ LANGUAGE plpgsql;


-- ------------------------------------------------------------------
-- the four shapes that work
-- ------------------------------------------------------------------
1:@db_name mpp_lr_dk: SELECT dk_sub('same');
1:@db_name mpp_lr_dk: SELECT dk_verdict('same') AS same_key;
1:@db_name mpp_lr_dk: SELECT dk_sub('narrow');
1:@db_name mpp_lr_dk: SELECT dk_verdict('narrow') AS target_keyed_on_fewer_columns;
1:@db_name mpp_lr_dk: SELECT dk_sub('full');
1:@db_name mpp_lr_dk: SELECT dk_verdict('full') AS source_sends_whole_rows;
1:@db_name mpp_lr_dk: SELECT dk_sub('insertonly');
1:@db_name mpp_lr_dk: SELECT dk_verdict('insertonly') AS source_has_no_replica_identity;

-- ------------------------------------------------------------------
-- and the four that cannot
-- ------------------------------------------------------------------
1:@db_name mpp_lr_dk: SELECT dk_sub('switch');
1:@db_name mpp_lr_dk: SELECT dk_verdict('switch') AS target_keyed_on_another_column;
1:@db_name mpp_lr_dk: SELECT dk_sub('randsrc');
1:@db_name mpp_lr_dk: SELECT dk_verdict('randsrc') AS source_distributed_randomly;
1:@db_name mpp_lr_dk: SELECT dk_sub('randtgt');
1:@db_name mpp_lr_dk: SELECT dk_verdict('randtgt') AS target_distributed_randomly;
1:@db_name mpp_lr_dk: SELECT dk_sub('nouniq');
1:@db_name mpp_lr_dk: SELECT dk_verdict('nouniq') AS target_without_a_unique_index;

-- the ones that were refused moved nothing
1:@db_name mpp_lr_dk: SELECT count(*) AS rows_on_the_refused_targets FROM (SELECT 1 FROM dk_switch UNION ALL SELECT 1 FROM dk_randsrc UNION ALL SELECT 1 FROM dk_randtgt UNION ALL SELECT 1 FROM dk_nouniq) x;

1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_same;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_narrow;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_full;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_insertonly;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_switch;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_randsrc;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_randtgt;
1:@db_name mpp_lr_dk: DROP SUBSCRIPTION dk_s_nouniq;
1:@db_name mpp_lr_dk: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('dk_sl_same');
SELECT gp_drop_replication_slot('dk_sl_narrow');
SELECT gp_drop_replication_slot('dk_sl_full');
SELECT gp_drop_replication_slot('dk_sl_insertonly');
DROP PUBLICATION dk_p_same, dk_p_narrow, dk_p_full, dk_p_insertonly, dk_p_switch, dk_p_randsrc, dk_p_randtgt, dk_p_nouniq;
DROP TABLE dk_same, dk_narrow, dk_full, dk_insertonly, dk_switch, dk_randsrc, dk_randtgt, dk_nouniq;
DROP DATABASE mpp_lr_dk;
