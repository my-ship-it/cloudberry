--
-- contrib/mpp_logicalrep/mpp_logicalrep_cdbinit.sql
--
-- The catalog side of MPP logical replication apply.  It is installed from
-- cdb_init.d rather than by CREATE EXTENSION because these tables have to be
-- coordinator-only, like the information_schema tables: the apply driver
-- writes them in the same distributed transaction as the batch it applied,
-- and reads them back from the coordinator.  A table created by an ordinary
-- CREATE TABLE would be distributed, its rows would live on the segments,
-- and the driver's own reads of them would find nothing.
--
-- initdb runs these scripts with allow_system_table_mods and
-- search_path = pg_catalog, which is what lets the distribution policy be
-- dropped and the execution locations be set.
--

-- Subscriptions this module owns.
--
-- CREATE SUBSCRIPTION ... WITH (mpp) is caught by the utility hook, which
-- lets the statement through without the option (and without connecting, so
-- the server's own launcher never starts a stock apply worker for it) and
-- records the subscription here.  This table, not pg_subscription.subenabled,
-- says whether a worker should be running.
--
-- unsupported_tables says what to do about a published table whose changes
-- the publisher cannot decode (anything that is not a heap table): 'error'
-- refuses the table and names it, 'snapshot' copies it once and stops
-- tracking it, which shows up as state 's' in gp_subscription_rel.
CREATE TABLE gp_mpp_subscription (
    subid   oid NOT NULL,
    subname name NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    disable_reason text,
    unsupported_tables text NOT NULL DEFAULT 'error'
);
DELETE FROM gp_distribution_policy WHERE localoid = 'gp_mpp_subscription'::regclass;
CREATE UNIQUE INDEX gp_mpp_subscription_subid_index ON gp_mpp_subscription (subid);

-- Coordinator-side apply state per subscription (design section 13.1).
CREATE TABLE gp_subscription_progress (
    subid           oid NOT NULL,
    coordinator_lsn pg_lsn,                -- source coordinator stream position
    seg_pos         int8[] NOT NULL,       -- per source segment: drained-to position
    seg_flush       int8[] NOT NULL,       -- per source segment: replay start = confirmed_flush
    updated         timestamptz NOT NULL DEFAULT now()
);
DELETE FROM gp_distribution_policy WHERE localoid = 'gp_subscription_progress'::regclass;
CREATE UNIQUE INDEX gp_subscription_progress_subid_index
    ON gp_subscription_progress (subid);

-- gxids applied above seg_flush, per source segment; trimmed as seg_flush advances
CREATE TABLE gp_subscription_applied (
    subid   oid NOT NULL,
    seg     int4 NOT NULL,
    gxid    int8 NOT NULL,
    seg_lsn int8 NOT NULL                  -- commit/prepare position on that segment
);
DELETE FROM gp_distribution_policy WHERE localoid = 'gp_subscription_applied'::regclass;
CREATE UNIQUE INDEX gp_subscription_applied_index
    ON gp_subscription_applied (subid, seg, gxid);

-- initial synchronisation state per relation (design section 10.6)
CREATE TABLE gp_subscription_rel (
    subid         oid NOT NULL,
    relid         oid NOT NULL,
    state         "char" NOT NULL,         -- i init, d data copy, f finished copy, c catch-up, r ready,
                                           -- s copied once and not tracked
                                           -- (see gp_mpp_subscription.unsupported_tables)
    sync_lsn      pg_lsn,
    sync_snapshot text,
    copy_job      text
);
DELETE FROM gp_distribution_policy WHERE localoid = 'gp_subscription_rel'::regclass;
CREATE UNIQUE INDEX gp_subscription_rel_index
    ON gp_subscription_rel (subid, relid);

--
-- The apply driver's SQL entry points (mpp_apply_sql.c).  Each of the first
-- three returns the same per-segment report: what the reader or writer gang
-- did.  They drive the gangs of the driver's own session, so they run on the
-- coordinator; gp_lr_explain returns a scalar, which CREATE FUNCTION will not
-- let declare an execution location, so its proexeclocation is set below.
--

-- Connect the reader gang to the source segments.
CREATE FUNCTION gp_lr_connect(
    IN subid oid, IN token int8, IN spec text,
    OUT seg int4, OUT kind "char", OUT gxid int8, OUT seg_lsn int8,
    OUT remote_relids oid[], OUT rel_names text[], OUT lockmode int2,
    OUT nrows int8, OUT nbytes int8)
RETURNS SETOF record
AS '$libdir/mpp_logicalrep', 'gp_lr_connect'
LANGUAGE C VOLATILE PARALLEL UNSAFE ROWS 3 EXECUTE ON COORDINATOR;

-- Release, read the streams and report ready transactions.
CREATE FUNCTION gp_lr_drain(
    IN subid oid, IN token int8, IN target_gxid int8, IN parts int4[],
    IN wait_ms int4, IN release_gxid int8[], IN release_local text[],
    OUT seg int4, OUT kind "char", OUT gxid int8, OUT seg_lsn int8,
    OUT remote_relids oid[], OUT rel_names text[], OUT lockmode int2,
    OUT nrows int8, OUT nbytes int8)
RETURNS SETOF record
AS '$libdir/mpp_logicalrep', 'gp_lr_drain'
LANGUAGE C VOLATILE PARALLEL UNSAFE CALLED ON NULL INPUT ROWS 10
EXECUTE ON COORDINATOR;

-- Apply the listed buffered transactions.
CREATE FUNCTION gp_lr_apply(
    IN subid oid, IN token int8, IN txns text[], IN defer oid[], IN catchup oid[],
    OUT seg int4, OUT kind "char", OUT gxid int8, OUT seg_lsn int8,
    OUT remote_relids oid[], OUT rel_names text[], OUT lockmode int2,
    OUT nrows int8, OUT nbytes int8)
RETURNS SETOF record
AS '$libdir/mpp_logicalrep', 'gp_lr_apply'
LANGUAGE C VOLATILE PARALLEL UNSAFE CALLED ON NULL INPUT ROWS 3
EXECUTE ON COORDINATOR;

-- EXPLAIN of a mode plan.
CREATE FUNCTION gp_lr_explain(subid oid, token int8, mode int4, spec text)
RETURNS text
AS '$libdir/mpp_logicalrep', 'gp_lr_explain'
LANGUAGE C VOLATILE PARALLEL UNSAFE;
UPDATE pg_proc SET proexeclocation = 'c'
 WHERE oid = 'gp_lr_explain(oid,int8,int4,text)'::regprocedure;

-- Initial sync: receive a segment-to-segment COPY stream (redistributed by
-- the enclosing INSERT .. SELECT).
CREATE FUNCTION gp_lr_copy_recv(rel regclass, job text, clients int4)
RETURNS SETOF record
AS '$libdir/mpp_logicalrep', 'gp_lr_copy_recv'
LANGUAGE C VOLATILE PARALLEL UNSAFE ROWS 1000 EXECUTE ON ALL SEGMENTS;

-- Initial sync: the copy helper listen port published by each target segment.
CREATE FUNCTION gp_lr_copy_ports(IN job text, OUT segid int4, OUT port int4)
RETURNS SETOF record
AS '$libdir/mpp_logicalrep', 'gp_lr_copy_ports'
LANGUAGE C VOLATILE PARALLEL UNSAFE ROWS 1 EXECUTE ON ALL SEGMENTS;

--
-- Record a committed batch: positions, plus the (segment, gxid, position)
-- triples applied; called inside the batch's transaction, before COMMIT.
--
CREATE FUNCTION gp_lr_progress(
    IN p_subid oid, IN p_coordinator_lsn pg_lsn,
    IN p_seg_pos int8[], IN p_seg_flush int8[],
    IN p_applied_seg int4[] DEFAULT NULL, IN p_applied_gxid int8[] DEFAULT NULL,
    IN p_applied_lsn int8[] DEFAULT NULL)
RETURNS void AS
$$
    INSERT INTO pg_catalog.gp_subscription_progress
        (subid, coordinator_lsn, seg_pos, seg_flush, updated)
    VALUES ($1, $2, $3, $4, now())
    ON CONFLICT (subid) DO UPDATE
        SET coordinator_lsn = EXCLUDED.coordinator_lsn,
            seg_pos = EXCLUDED.seg_pos,
            seg_flush = EXCLUDED.seg_flush,
            updated = now();
    INSERT INTO pg_catalog.gp_subscription_applied (subid, seg, gxid, seg_lsn)
    SELECT $1, a.s, a.g, a.l FROM unnest($5, $6, $7) AS a(s, g, l)
    ON CONFLICT DO NOTHING;
    -- below seg_flush everything is applied by definition: forget it
    DELETE FROM pg_catalog.gp_subscription_applied a
     WHERE a.subid = $1 AND a.seg_lsn < $4[a.seg + 1];
$$ LANGUAGE sql VOLATILE;
