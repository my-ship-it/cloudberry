-- Order within one source segment.
--
-- A segment's stream is applied in its own order, and that order is the only
-- one there is for a transaction with no distributed identity: a utility-mode
-- write has no gxid, so nothing but its position in that segment's stream says
-- when it happened.  Mixed with the distributed transactions on the same
-- segment, the two have to interleave exactly as the segment wrote them.
--
-- Each step appends to the value rather than replacing it, so the final string
-- is the order itself: applying the same steps in any other order leaves a
-- different value, and replication carries whole rows, so a target that
-- reorders cannot end up with the right one by luck.
--
--   1  a chain of utility writes to one row: the last one wins
--   2  utility and distributed writes to the same row, alternating
--   3  the same over many rows at once
--
-- Three primaries, as the rest of isolation2 assumes: segment 0 is written to
-- through a utility connection.

SELECT count(*) = 3 AS three_primaries
  FROM gp_segment_configuration WHERE role = 'p' AND content >= 0;

CREATE DATABASE mpp_lr_ord;

CREATE TABLE ord_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
INSERT INTO ord_t SELECT g, 'seed' FROM generate_series(1, 300) g;
CREATE PUBLICATION ord_pub FOR TABLE ord_t;

1:@db_name mpp_lr_ord: CREATE TABLE ord_t (id int PRIMARY KEY, v text) DISTRIBUTED BY (id);
1:@db_name mpp_lr_ord: CREATE FUNCTION lr_wait(cond text) RETURNS bool AS $$ DECLARE ok bool; BEGIN FOR i IN 1 .. 1800 LOOP EXECUTE cond INTO ok; IF ok THEN RETURN true; END IF; PERFORM pg_sleep(0.1); END LOOP; RETURN false; END $$ LANGUAGE plpgsql;
1:@db_name mpp_lr_ord: DO $$ BEGIN EXECUTE format('CREATE SUBSCRIPTION ord_sub CONNECTION %L PUBLICATION ord_pub WITH (slot_name = ''ord_slot'')', 'dbname=mpp_lr_source port=' || current_setting('port')); END $$;
1:@db_name mpp_lr_ord: SELECT lr_wait($$SELECT count(*) = 1 FROM gp_subscription_rel r JOIN pg_subscription s ON s.oid = r.subid WHERE s.subname = 'ord_sub' AND r.relid <> 0 AND r.state = 'r'$$) AS synced;
1:@db_name mpp_lr_ord: SELECT count(*) AS copied FROM ord_t;

-- ------------------------------------------------------------------
-- 1. a chain of utility writes to one row
-- ------------------------------------------------------------------
-- Each of these is a transaction of its own on segment 0, with no gxid.  In
-- utility mode the subquery sees that segment's rows alone, so all three hit
-- the same row -- the one an ordinary query reaches as the lowest id living
-- on segment 0.
0U: UPDATE ord_t SET v = v || '-u1' WHERE id = (SELECT min(id) FROM ord_t);
0U: UPDATE ord_t SET v = v || '-u2' WHERE id = (SELECT min(id) FROM ord_t);
0U: UPDATE ord_t SET v = v || '-u3' WHERE id = (SELECT min(id) FROM ord_t);
1:@db_name mpp_lr_ord: SELECT lr_wait($$SELECT count(*) = 1 FROM ord_t WHERE v = 'seed-u1-u2-u3'$$) AS chain_applied_in_order;
SELECT v AS on_the_publisher FROM ord_t WHERE id = (SELECT min(id) FROM ord_t WHERE gp_segment_id = 0);
1:@db_name mpp_lr_ord: SELECT count(*) AS out_of_order FROM ord_t WHERE v LIKE 'seed-u%' AND v <> 'seed-u1-u2-u3';

-- ------------------------------------------------------------------
-- 2. utility and distributed writes to the same row, alternating
-- ------------------------------------------------------------------
-- The distributed one reaches the same row from the coordinator, by asking
-- for the lowest id on segment 0.  It has a gxid; the utility ones have none.
UPDATE ord_t SET v = v || '-d1' WHERE id = (SELECT min(id) FROM ord_t WHERE gp_segment_id = 0);
0U: UPDATE ord_t SET v = v || '-u4' WHERE id = (SELECT min(id) FROM ord_t);
UPDATE ord_t SET v = v || '-d2' WHERE id = (SELECT min(id) FROM ord_t WHERE gp_segment_id = 0);
0U: UPDATE ord_t SET v = v || '-u5' WHERE id = (SELECT min(id) FROM ord_t);
1:@db_name mpp_lr_ord: SELECT lr_wait($$SELECT count(*) = 1 FROM ord_t WHERE v = 'seed-u1-u2-u3-d1-u4-d2-u5'$$) AS interleaved_in_order;
SELECT v AS on_the_publisher FROM ord_t WHERE id = (SELECT min(id) FROM ord_t WHERE gp_segment_id = 0);

-- ------------------------------------------------------------------
-- 3. the same over every row of that segment at once
-- ------------------------------------------------------------------
0U: UPDATE ord_t SET v = v || '-a';
UPDATE ord_t SET v = v || '-b' WHERE gp_segment_id = 0;
0U: UPDATE ord_t SET v = v || '-c';
UPDATE ord_t SET v = v || '-d' WHERE gp_segment_id = 0;
0U: UPDATE ord_t SET v = v || '-e';
-- A last one over every segment, so that waiting for it is waiting for all of
-- the above: commit order is preserved, so when this has been applied
-- everything committed before it has been too.  Waiting on the rows of
-- segment 0 alone would not do -- how many there are is the cluster's
-- business, and a count that is still zero satisfies any comparison with
-- itself.
UPDATE ord_t SET v = v || '-done';
1:@db_name mpp_lr_ord: SELECT lr_wait($$SELECT count(*) = 300 FROM ord_t WHERE v LIKE '%-done'$$) AS all_of_it_applied;
-- every row segment 0 held ends with the same five steps, in that order
1:@db_name mpp_lr_ord: SELECT count(*) > 0 AS segment_0_rows_seen, count(*) FILTER (WHERE v NOT LIKE '%-a-b-c-d-e-done') AS ending_wrong FROM ord_t WHERE v LIKE '%-a%';
SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM ord_t;
1:@db_name mpp_lr_ord: SELECT count(*) AS rows, sum(hashtext(id || '|' || v)) AS digest FROM ord_t;
1:@db_name mpp_lr_ord: SELECT phase, stuck_errors, recoveries FROM gp_stat_mpp_subscription WHERE subname = 'ord_sub';

0Uq:
1:@db_name mpp_lr_ord: DROP SUBSCRIPTION ord_sub;
1q:
SELECT gp_drop_replication_slot('ord_slot');
DROP PUBLICATION ord_pub;
DROP TABLE ord_t;
DROP DATABASE mpp_lr_ord;
