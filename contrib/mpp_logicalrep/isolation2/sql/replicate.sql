-- One subscription, end to end, inside one cluster: the test database is the
-- publisher and a database of its own is the subscriber.  This is what the
-- scripts under test/cluster/ do between two clusters, for the part that does
-- not need two of them.

CREATE DATABASE mpp_lr_target;

-- the publisher: a heap table, a publication, and the cluster-wide slots
CREATE TABLE lr_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO lr_t SELECT g, 'seed' FROM generate_series(1, 200) g;
CREATE PUBLICATION lr_pub FOR TABLE lr_t;
SELECT count(*) > 0 AS slots_created
  FROM gp_create_logical_replication_slot('lr_slot', 'pgoutput', false, true);

-- the subscriber
1:@db_name mpp_lr_target: CREATE TABLE lr_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);

-- wait for a condition rather than sleep for a guess
1:@db_name mpp_lr_target: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1200 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- the publisher is this same cluster, so it is reached on the same port
1:@db_name mpp_lr_target: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION lr_sub CONNECTION %L PUBLICATION lr_pub WITH (slot_name = ''lr_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;

-- the initial copy
1:@db_name mpp_lr_target: SELECT lr_wait('SELECT count(*) = 200 FROM lr_t') AS copied;

-- and then the stream
INSERT INTO lr_t SELECT g, 'more' FROM generate_series(201, 260) g;
UPDATE lr_t SET v = 'changed' WHERE id % 50 = 0;
DELETE FROM lr_t WHERE id % 97 = 0;
SELECT count(*) AS on_the_publisher FROM lr_t;

1:@db_name mpp_lr_target: SELECT lr_wait('SELECT count(*) = 258 FROM lr_t') AS caught_up;
1:@db_name mpp_lr_target: SELECT count(*) AS rows, count(*) FILTER (WHERE v = 'changed') AS changed FROM lr_t;

-- what it looks like while it runs
1:@db_name mpp_lr_target: SELECT phase, tables, tables_ready, recoveries, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'lr_sub';

-- and away
1:@db_name mpp_lr_target: DROP SUBSCRIPTION lr_sub;
1q:
SELECT gp_drop_replication_slot('lr_slot');
DROP PUBLICATION lr_pub;
DROP TABLE lr_t;
DROP DATABASE mpp_lr_target;
