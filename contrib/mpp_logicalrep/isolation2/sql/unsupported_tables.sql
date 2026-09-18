-- A published table whose changes the publisher cannot decode.  Only heap
-- tables produce logical decoding output: the append-optimised access methods
-- have no decoding routine at all, so such a table would be copied once by the
-- initial synchronisation and then quietly stop being true.

CREATE DATABASE mpp_lr_a1;

CREATE TABLE a1_h  (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
CREATE TABLE a1_ao (id int PRIMARY KEY, v text) USING ao_row DISTRIBUTED BY (id);
INSERT INTO a1_h  SELECT g, 'seed' FROM generate_series(1, 100) g;
INSERT INTO a1_ao SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION a1_pub FOR TABLE a1_h, a1_ao;
SELECT count(*) > 0 AS slots FROM gp_create_logical_replication_slot('a1_slot', 'pgoutput', false, true);

1:@db_name mpp_lr_a1: CREATE TABLE a1_h  (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_a1: CREATE TABLE a1_ao (id int PRIMARY KEY, v text) USING ao_row DISTRIBUTED BY (id);
-- Waits here poll the subscription's own catalogs, never a table that is
-- being synchronised: a lock taken inside a function is held until the
-- function returns, and the synchronising COPY truncates its target first, so
-- a poll of the table being copied would block the copy it is waiting for for
-- as long as it waits.  Once a table's state is settled its rows are already
-- committed, and a plain statement outside the function can count them.
1:@db_name mpp_lr_a1: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1200 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- 1. the default refuses, and says which table and which access method
1:@db_name mpp_lr_a1: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION a1_sub CONNECTION %L PUBLICATION a1_pub WITH (slot_name = ''a1_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_a1: SELECT unsupported_tables AS the_default FROM gp_mpp_subscription WHERE subname = 'a1_sub';
1:@db_name mpp_lr_a1: SELECT lr_wait('SELECT NOT enabled FROM gp_mpp_subscription WHERE subname = ''a1_sub''') AS disabled_itself;
1:@db_name mpp_lr_a1: SELECT disable_reason LIKE '%a1_ao%' AS names_the_table, disable_reason LIKE '%ao_row%' AS names_the_method FROM gp_mpp_subscription WHERE subname = 'a1_sub';
1:@db_name mpp_lr_a1: SELECT count(*) AS nothing_was_copied FROM gp_subscription_rel WHERE subid = (SELECT oid FROM pg_subscription WHERE subname = 'a1_sub');

-- 2. 'snapshot' copies it once and stops tracking it
1:@db_name mpp_lr_a1: ALTER SUBSCRIPTION a1_sub SET (unsupported_tables = 'snapshot');
1:@db_name mpp_lr_a1: ALTER SUBSCRIPTION a1_sub ENABLE;
1:@db_name mpp_lr_a1: SELECT lr_wait($$SELECT count(*) = 2 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'a1_sub' AND r.relid <> 0 AND r.state IN ('r','s')$$) AS states_settled;
-- 'r' streams from here on, 's' was copied once and is not tracked
1:@db_name mpp_lr_a1: SELECT c.relname AS rel, r.state FROM gp_subscription_rel r JOIN pg_class c ON c.oid = r.relid JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'a1_sub' ORDER BY 1;
1:@db_name mpp_lr_a1: SELECT count(*) AS heap_copied FROM a1_h;
1:@db_name mpp_lr_a1: SELECT count(*) AS ao_copied FROM a1_ao;

-- both change on the publisher; only the heap one may follow.  Polling a
-- table that is streaming is safe: applying rows to it takes no lock that a
-- reader conflicts with.
INSERT INTO a1_h  SELECT g, 'new' FROM generate_series(101, 150) g;
INSERT INTO a1_ao SELECT g, 'new' FROM generate_series(101, 150) g;
1:@db_name mpp_lr_a1: SELECT lr_wait('SELECT count(*) = 150 FROM a1_h') AS heap_followed;
1:@db_name mpp_lr_a1: SELECT count(*) AS ao_stayed_as_copied FROM a1_ao;
1:@db_name mpp_lr_a1: SELECT enabled AS still_enabled FROM gp_mpp_subscription WHERE subname = 'a1_sub';

-- 3. REFRESH is refused on the spot in 'error' mode, and warns in 'snapshot'
CREATE TABLE a1_ao2 (id int PRIMARY KEY, v text) USING ao_column DISTRIBUTED BY (id);
INSERT INTO a1_ao2 SELECT g, 'seed' FROM generate_series(1, 40) g;
ALTER PUBLICATION a1_pub ADD TABLE a1_ao2;
1:@db_name mpp_lr_a1: CREATE TABLE a1_ao2 (id int PRIMARY KEY, v text) USING ao_column DISTRIBUTED BY (id);
1:@db_name mpp_lr_a1: ALTER SUBSCRIPTION a1_sub SET (unsupported_tables = 'error');
1:@db_name mpp_lr_a1: ALTER SUBSCRIPTION a1_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_a1: SELECT count(*) AS no_state_row FROM gp_subscription_rel r JOIN pg_class c ON c.oid = r.relid JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'a1_sub' AND c.relname = 'a1_ao2';
1:@db_name mpp_lr_a1: ALTER SUBSCRIPTION a1_sub SET (unsupported_tables = 'snapshot');
1:@db_name mpp_lr_a1: ALTER SUBSCRIPTION a1_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_a1: SELECT lr_wait($$SELECT r.state = 's' FROM gp_subscription_rel r JOIN pg_class c ON c.oid = r.relid JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'a1_sub' AND c.relname = 'a1_ao2'$$) AS ended_as_snapshot;
1:@db_name mpp_lr_a1: SELECT count(*) AS added_table_copied FROM a1_ao2;

-- and the heap table is still streaming after all that
INSERT INTO a1_h SELECT g, 'last' FROM generate_series(200, 210) g;
1:@db_name mpp_lr_a1: SELECT lr_wait('SELECT count(*) = 161 FROM a1_h') AS heap_still_following;

1:@db_name mpp_lr_a1: DROP SUBSCRIPTION a1_sub;
1q:
SELECT gp_drop_replication_slot('a1_slot');
DROP PUBLICATION a1_pub;
DROP TABLE a1_h, a1_ao, a1_ao2;
DROP DATABASE mpp_lr_a1;
