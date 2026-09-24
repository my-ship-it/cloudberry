-- A publication FOR ALL TABLES, and a table created after the subscription.
--
-- Every other case here names its tables.  FOR ALL TABLES does not: the
-- publication is a standing order, so a table created on the publisher later
-- is in it already, and what decides whether the subscriber copies it is
-- ALTER SUBSCRIPTION ... REFRESH PUBLICATION alone.  It also takes the
-- publisher's word for what exists, which is the trap: a table the target
-- has not got is in the publication too, and there is no ADD TABLE to think
-- twice at.
--
--   1  FOR ALL TABLES synchronises what the publisher has
--   2  a table created afterwards is in the publication, and is copied on
--      the next REFRESH -- without it, it is not
--   3  the other order -- created on the publisher and written to before
--      the subscriber has it -- stops the apply, which names the relation
--      and disables the subscription rather than applying the rest and
--      leaving a hole
--   4  the way out is to create it on the target.  Dropping it on the
--      publisher is not: by then the change is in the stream, and a worker
--      that starts again starts from before it and meets it again
--
-- Its own source database, because FOR ALL TABLES means all of them and the
-- database every other case uses is shared.

CREATE DATABASE mpp_lr_at_src;
CREATE DATABASE mpp_lr_at;

2:@db_name mpp_lr_at_src: CREATE TABLE at_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
2:@db_name mpp_lr_at_src: CREATE TABLE at_b (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
2:@db_name mpp_lr_at_src: INSERT INTO at_a SELECT g, 'seed' FROM generate_series(1, 400) g;
2:@db_name mpp_lr_at_src: INSERT INTO at_b SELECT g, 'seed' FROM generate_series(1, 300) g;
2:@db_name mpp_lr_at_src: CREATE PUBLICATION at_pub FOR ALL TABLES;

1:@db_name mpp_lr_at: CREATE TABLE at_a (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_at: CREATE TABLE at_b (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_at: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_at: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_at: CREATE FUNCTION lr_ready() RETURNS text AS $$ SELECT coalesce(string_agg(c.relname || '=' || cast(r.state as text), ' ' ORDER BY c.relname), 'none') FROM gp_subscription_rel r JOIN pg_class c ON c.oid = r.relid WHERE r.relid <> 0 $$ LANGUAGE sql;

-- ------------------------------------------------------------------
-- 1. what the publisher has when the subscription is made
-- ------------------------------------------------------------------
1:@db_name mpp_lr_at: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION at_sub CONNECTION %L PUBLICATION at_pub WITH (slot_name = ''at_slot'')', 'dbname=mpp_lr_at_src port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_at: SELECT lr_wait($$SELECT lr_ready() = 'at_a=r at_b=r'$$) AS both_ready;
1:@db_name mpp_lr_at: SELECT count(*) AS a_copied FROM at_a;
1:@db_name mpp_lr_at: SELECT count(*) AS b_copied FROM at_b;

-- ------------------------------------------------------------------
-- 2. a table created afterwards is in the publication already
-- ------------------------------------------------------------------
-- The target first.  With FOR ALL TABLES the publisher's first write to a
-- new table is on the stream whether or not the subscriber has the table,
-- and a subscriber that has not got it stops there (3 below) -- so creating
-- it on the publisher and then hurrying to create it here is a race, and
-- the order that is not a race is this one.
1:@db_name mpp_lr_at: CREATE TABLE at_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
2:@db_name mpp_lr_at_src: CREATE TABLE at_c (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
2:@db_name mpp_lr_at_src: INSERT INTO at_c SELECT g, 'made later' FROM generate_series(1, 200) g;
2:@db_name mpp_lr_at_src: SELECT count(*) = 3 AS three_in_the_publication FROM pg_publication_tables WHERE pubname = 'at_pub';

-- the publication carries it, but nothing has told the subscription yet
1:@db_name mpp_lr_at: SELECT lr_ready() AS before_refresh;
1:@db_name mpp_lr_at: ALTER SUBSCRIPTION at_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_at: SELECT lr_wait($$SELECT lr_ready() = 'at_a=r at_b=r at_c=r'$$) AS three_ready;
1:@db_name mpp_lr_at: SELECT lr_wait('SELECT count(*) = 200 FROM at_c') AS c_copied;

-- and it streams from then on, as do the ones that were there first
2:@db_name mpp_lr_at_src: INSERT INTO at_c SELECT 1000 + g, 'streamed' FROM generate_series(1, 20) g;
2:@db_name mpp_lr_at_src: INSERT INTO at_a SELECT 1000 + g, 'streamed' FROM generate_series(1, 20) g;
1:@db_name mpp_lr_at: SELECT lr_wait('SELECT count(*) = 220 FROM at_c') AS c_streams;
1:@db_name mpp_lr_at: SELECT lr_wait('SELECT count(*) = 420 FROM at_a') AS a_still_streams;

-- ------------------------------------------------------------------
-- 3. the other order: the publisher first
-- ------------------------------------------------------------------
-- Nothing here creates at_late on the target.  Its first write is on the
-- stream all the same -- FOR ALL TABLES put it in the publication the
-- moment it existed -- and the apply has nowhere to put it.
2:@db_name mpp_lr_at_src: CREATE TABLE at_late (id int, v text) DISTRIBUTED BY (id);
2:@db_name mpp_lr_at_src: INSERT INTO at_late VALUES (1, 'written before the target had it');
1:@db_name mpp_lr_at: ALTER SUBSCRIPTION at_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_at: SELECT lr_wait($$SELECT disable_reason LIKE '%at_late%' FROM gp_stat_mpp_subscription WHERE subname = 'at_sub'$$) AS it_says_which_one;
1:@db_name mpp_lr_at: SELECT enabled FROM gp_stat_mpp_subscription WHERE subname = 'at_sub';
-- the tables that were already streaming kept what they had
1:@db_name mpp_lr_at: SELECT count(*) AS a_intact FROM at_a;

-- ------------------------------------------------------------------
-- 4. creating it on the target is the way out
-- ------------------------------------------------------------------
-- The REFRESH above failed too, so the subscription never heard of it;
-- what stopped the apply is the stream, which carries the row whether or
-- not the subscription tracks the table.  Dropping it on the publisher
-- would not help: the change is already in the WAL the slot holds, and a
-- worker that starts again starts from before it and meets it again.
--
-- The copy that follows truncates first, so the row arrives exactly once
-- however much of it the stream had already tried to apply.
1:@db_name mpp_lr_at: CREATE TABLE at_late (id int, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_at: ALTER SUBSCRIPTION at_sub REFRESH PUBLICATION;
1:@db_name mpp_lr_at: ALTER SUBSCRIPTION at_sub ENABLE;
1:@db_name mpp_lr_at: SELECT lr_wait($$SELECT lr_ready() = 'at_a=r at_b=r at_c=r at_late=r'$$) AS four_ready;
1:@db_name mpp_lr_at: SELECT lr_wait('SELECT count(*) = 1 FROM at_late') AS it_arrived_once;
2:@db_name mpp_lr_at_src: INSERT INTO at_a SELECT 2000 + g, 'after the trouble' FROM generate_series(1, 10) g;
1:@db_name mpp_lr_at: SELECT lr_wait('SELECT count(*) = 430 FROM at_a') AS streaming_again;
1:@db_name mpp_lr_at: SELECT lr_wait($$SELECT phase = 'streaming' FROM gp_stat_mpp_subscription WHERE subname = 'at_sub'$$) AS streaming;
1:@db_name mpp_lr_at: SELECT enabled, stuck_errors FROM gp_stat_mpp_subscription WHERE subname = 'at_sub';

1:@db_name mpp_lr_at: DROP SUBSCRIPTION at_sub;
1:@db_name mpp_lr_at: SELECT lr_worker_gone() AS worker_gone;
1q:
2:@db_name mpp_lr_at_src: DROP TABLE at_late;
2:@db_name mpp_lr_at_src: SELECT gp_drop_replication_slot('at_slot');
2:@db_name mpp_lr_at_src: DROP PUBLICATION at_pub;
2q:
DROP DATABASE mpp_lr_at;
DROP DATABASE mpp_lr_at_src;
