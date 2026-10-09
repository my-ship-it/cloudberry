-- An idle subscription must not pin the source's WAL.
--
-- What is confirmed back to the source is the position it is safe to resume
-- from.  While something is still waiting to be applied that is where the
-- last batch committed; once nothing is, it is everything that was received
-- -- otherwise a subscription with nothing left to do holds every byte of WAL
-- written since its last batch, for ever, and the source cannot be shut down
-- either, because a walsender only leaves once its client has confirmed what
-- it sent.
--
-- The WAL is written in another database, and so is everything that watches
-- it.  A write in the subscribed database would not do, published or not: it
-- commits distributedly, the coordinator writes the event, and the driver
-- advances past it on its own when no segment reports anything for it.
-- Another database's commits are filtered out of the slot by their database
-- (DB1), so the walsender moves and the driver hears nothing -- which is the
-- case that used to freeze the confirmation.

CREATE DATABASE mpp_lr_idle;
CREATE DATABASE mpp_lr_idle_noise;

CREATE TABLE idl_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO idl_t SELECT g, 'seed' FROM generate_series(1, 100) g;
CREATE PUBLICATION idl_pub FOR TABLE idl_t;

1:@db_name mpp_lr_idle: CREATE TABLE idl_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_idle: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 600 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_idle: CREATE FUNCTION lr_worker_gone() RETURNS bool AS $$ DECLARE n int; BEGIN FOR i IN 1 .. 300 LOOP PERFORM pg_stat_clear_snapshot(); SELECT count(*) INTO n FROM pg_stat_activity WHERE backend_type = 'mpp apply worker' AND datname = current_database(); IF n = 0 THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_idle: CREATE FUNCTION lr_ready(sub text) RETURNS text AS $$ SELECT coalesce(string_agg(c.relname || '=' || cast(r.state as text), ' ' ORDER BY c.relname), 'none') FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid JOIN pg_class c ON c.oid = r.relid WHERE s.subname = $1 $$ LANGUAGE sql;

1:@db_name mpp_lr_idle: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION idl_sub CONNECTION %L PUBLICATION idl_pub WITH (slot_name = ''idl_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_idle: SELECT lr_wait($$SELECT lr_ready('idl_sub') = 'idl_t=r'$$) AS ready;

-- From here on, nothing touches the subscribed database.  pg_replication_slots
-- is visible from any of them.
2:@db_name mpp_lr_idle_noise: CREATE TABLE idl_mark (lsn pg_lsn);
2:@db_name mpp_lr_idle_noise: CREATE TABLE idl_noise (id int, pad text) DISTRIBUTED BY (id);
2:@db_name mpp_lr_idle_noise: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 600 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;

-- where the slot stands now that the subscription has nothing left to do
2:@db_name mpp_lr_idle_noise: INSERT INTO idl_mark SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'idl_slot';

-- write WAL the subscription will never hear of; the slot must follow it
2:@db_name mpp_lr_idle_noise: INSERT INTO idl_noise SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g;
2:@db_name mpp_lr_idle_noise: SELECT lr_wait($$SELECT confirmed_flush_lsn > (SELECT lsn FROM idl_mark) FROM pg_replication_slots WHERE slot_name = 'idl_slot'$$) AS followed_the_noise;

-- again, so that it is following rather than having moved once
2:@db_name mpp_lr_idle_noise: UPDATE idl_mark SET lsn = (SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = 'idl_slot');
2:@db_name mpp_lr_idle_noise: INSERT INTO idl_noise SELECT g, repeat('y', 100) FROM generate_series(1, 1000) g;
2:@db_name mpp_lr_idle_noise: SELECT lr_wait($$SELECT confirmed_flush_lsn > (SELECT lsn FROM idl_mark) FROM pg_replication_slots WHERE slot_name = 'idl_slot'$$) AS followed_again;
2q:

-- and it is still a working subscription
INSERT INTO idl_t SELECT g, 'more' FROM generate_series(101, 200) g;
1:@db_name mpp_lr_idle: SELECT lr_wait('SELECT count(*) = 200 FROM idl_t') AS still_applying;

1:@db_name mpp_lr_idle: DROP SUBSCRIPTION idl_sub;
1:@db_name mpp_lr_idle: SELECT lr_worker_gone() AS worker_gone;
1q:
SELECT gp_drop_replication_slot('idl_slot');
DROP PUBLICATION idl_pub;
DROP TABLE idl_t;
DROP DATABASE mpp_lr_idle;
DROP DATABASE mpp_lr_idle_noise;
