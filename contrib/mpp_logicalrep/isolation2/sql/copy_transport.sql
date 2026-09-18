-- The segment-to-segment COPY transport (copyhelper/) with the planner's
-- redistribution on top, which the initial synchronisation is built on:
--
--   src --COPY TO PROGRAM send ON SEGMENT--> recv helper
--       --> gp_lr_copy_recv() --> INSERT INTO tgt (Redistribute Motion) --> tgt
--
--   A  N -> N: one COPY ... ON SEGMENT, each source segment feeding one target
--   B  2N -> N: two COPY ... ON SEGMENT runs, rows split by parity, so the
--      second acts as source segments N..2N-1 and every target takes two
--      clients
--
-- The 2 -> N case, where a target segment expects no client at all, drives the
-- helper binary by hand from a shell pipe and stays in
-- test/cluster/copy_transport.sh.
--
-- Everything here is one database: the transport knows nothing of
-- subscriptions.

CREATE TABLE ct_src (id int, val text) DISTRIBUTED BY (id);
CREATE TABLE ct_tgt (id int, val text) DISTRIBUTED BY (id);
INSERT INTO ct_src SELECT g, 'row-' || g || '-payload-' || md5(g::text) FROM generate_series(1, 20000) g;

-- how many primaries there are, so the test does not care how big the cluster is
CREATE FUNCTION ct_nseg() RETURNS int AS $$ SELECT cast(count(*) as int) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0 $$ LANGUAGE sql;

-- The receiver publishes one port per target segment; wait for them all and
-- return the address list the senders are pointed at.
CREATE FUNCTION ct_targets(job text, want int) RETURNS text AS $$ DECLARE t text; BEGIN FOR i IN 1 .. 600 LOOP SELECT string_agg(a.address || ':' || p.port, ',' ORDER BY p.segid) INTO t FROM gp_lr_copy_ports(job) p JOIN gp_segment_configuration a ON a.content = p.segid AND a.role = 'p' HAVING count(*) = want; EXIT WHEN t IS NOT NULL; PERFORM pg_sleep(0.1); END LOOP; RETURN t; END $$ LANGUAGE plpgsql;

CREATE FUNCTION ct_send(job text, pred text) RETURNS text AS $$ DECLARE t text; BEGIN SELECT ct_targets(job, ct_nseg()) INTO t; IF t IS NULL THEN RETURN 'the receiver never published its ports'; END IF; EXECUTE format('COPY (SELECT * FROM ct_src WHERE %s) TO PROGRAM %L ON SEGMENT', pred, 'cbdb_lr_copyhelper send --job ' || job || ' --seg <SEGID> --targets ' || t || ' --crc'); RETURN 'sent'; END $$ LANGUAGE plpgsql;

CREATE FUNCTION ct_recv(job text, nsources int) RETURNS void AS $$ BEGIN EXECUTE format('INSERT INTO ct_tgt SELECT * FROM gp_lr_copy_recv(cast(''ct_tgt'' as regclass), %L, %s) AS x(id int, val text)', job, nsources); END $$ LANGUAGE plpgsql;

-- src == tgt as multisets, both ways, plus an order-independent checksum
CREATE FUNCTION ct_same() RETURNS text AS $$ SELECT 'rows=' || (SELECT count(*) FROM ct_tgt) || ' checksums_equal=' || ((SELECT sum(hashtext(id || '|' || val)) FROM ct_src) IS NOT DISTINCT FROM (SELECT sum(hashtext(id || '|' || val)) FROM ct_tgt)) || ' no_row_either_way=' || ((SELECT count(*) FROM (SELECT * FROM ct_src EXCEPT ALL SELECT * FROM ct_tgt) a) = 0 AND (SELECT count(*) FROM (SELECT * FROM ct_tgt EXCEPT ALL SELECT * FROM ct_src) b) = 0) $$ LANGUAGE sql;

-- every target segment got some of it
CREATE FUNCTION ct_spread() RETURNS bool AS $$ SELECT count(DISTINCT gp_segment_id) = ct_nseg() FROM ct_tgt $$ LANGUAGE sql;

-- ------------------------------------------------------------------
-- A. one source segment per target segment
-- ------------------------------------------------------------------
2&: SELECT ct_recv('ct_a', ct_nseg());
1: SELECT ct_send('ct_a', 'true') AS phase_a;
2<:
1: SELECT ct_same() AS phase_a_result;
1: SELECT ct_spread() AS every_segment_received;

-- ------------------------------------------------------------------
-- B. twice as many source segments as targets
-- ------------------------------------------------------------------
1: TRUNCATE ct_tgt;
2&: SELECT ct_recv('ct_b', 2 * ct_nseg());
1: SELECT ct_send('ct_b', 'id % 2 = 0') AS first_half;
1: SELECT ct_send('ct_b', 'id % 2 = 1') AS second_half;
2<:
1: SELECT ct_same() AS phase_b_result;
1: SELECT ct_spread() AS every_segment_received;

-- nothing of either job is left behind
1: SELECT count(*) AS ports_left FROM gp_lr_copy_ports('ct_a');
1: SELECT count(*) AS ports_left FROM gp_lr_copy_ports('ct_b');

1q:
2q:
DROP FUNCTION ct_same(), ct_spread(), ct_send(text, text), ct_recv(text, int), ct_targets(text, int), ct_nseg();
DROP TABLE ct_src, ct_tgt;
