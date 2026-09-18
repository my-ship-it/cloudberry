--
-- The subscription DDL this module takes over.
--
-- Only the statement and what it leaves in the catalogs: no slots, no
-- publisher, nothing started.  Every subscription here is created disabled,
-- so the launcher never has anything to do and the test is about the
-- contract alone.
--

-- the module has to be loaded, or none of this means anything
SELECT current_setting('shared_preload_libraries') LIKE '%mpp_logicalrep%'
       AS module_is_loaded;

-- Every CREATE SUBSCRIPTION below leaves the server's own row unconnected
-- and says so, once per node; the text carries host, port and pid, so it
-- would differ on every cluster.  Errors are what this test is about.
SET client_min_messages = error;

-- ------------------------------------------------------------------
-- options the module owns, and refuses
-- ------------------------------------------------------------------

-- connect is how the module keeps the server's own machinery out of the
-- way, so it is not the user's to set, in either direction
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = 'x', connect = false, enabled = false);
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = 'x', connect = true, enabled = false);

-- the slots are cluster-wide and only gp_create_logical_replication_slot()
-- can make them
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = 'x', create_slot = true, enabled = false);

-- and it needs to be told which slots to stream from
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (enabled = false);
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = NONE, enabled = false);

-- unsupported_tables takes two values and nothing else
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = 'x', enabled = false, unsupported_tables = 'maybe');

-- there is no option for asking the server to apply a subscription itself:
-- a loaded module means every subscription here is the module's
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (mpp = false, slot_name = 'x', enabled = false);
CREATE SUBSCRIPTION s_bad CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (mpp, slot_name = 'x', enabled = false);

-- ------------------------------------------------------------------
-- what a good one leaves behind
-- ------------------------------------------------------------------

-- nothing connects, so this is allowed in a transaction block
BEGIN;
CREATE SUBSCRIPTION s1 CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = 'slot_s1', enabled = false);
COMMIT;

-- the server's own row stays disabled: that is what keeps its launcher off
-- this subscription, and the module's registry is what says it is ours
SELECT subenabled, subslotname, subconninfo IS NOT NULL AS has_conninfo
  FROM pg_subscription WHERE subname = 's1';
SELECT subname, enabled, unsupported_tables, disable_reason
  FROM pg_catalog.gp_mpp_subscription WHERE subname = 's1';

-- a second one, to show the registry holds one row per subscription
CREATE SUBSCRIPTION s2 CONNECTION 'dbname=nowhere' PUBLICATION p
    WITH (slot_name = 'slot_s2', enabled = false);
SELECT subname, enabled FROM pg_catalog.gp_mpp_subscription WHERE subname = 's2';

-- ------------------------------------------------------------------
-- ALTER: the registry is what changes, not pg_subscription
-- ------------------------------------------------------------------

ALTER SUBSCRIPTION s1 SET (unsupported_tables = 'snapshot');
SELECT unsupported_tables FROM pg_catalog.gp_mpp_subscription WHERE subname = 's1';
ALTER SUBSCRIPTION s1 SET (unsupported_tables = 'sometimes');

ALTER SUBSCRIPTION s1 DISABLE;
SELECT enabled FROM pg_catalog.gp_mpp_subscription WHERE subname = 's1';
SELECT subenabled AS still_false_in_pg_subscription
  FROM pg_subscription WHERE subname = 's1';

-- ------------------------------------------------------------------
-- DROP: the state rows go, the publisher's slots do not
-- ------------------------------------------------------------------

-- dropping says the publisher's slots are still there, which is the point
SET client_min_messages = notice;
DROP SUBSCRIPTION s1;
SET client_min_messages = error;
SELECT count(*) AS registry_rows
  FROM pg_catalog.gp_mpp_subscription WHERE subname = 's1';
SELECT count(*) AS state_rows FROM pg_catalog.gp_subscription_rel
 WHERE subid NOT IN (SELECT oid FROM pg_subscription);

DROP SUBSCRIPTION s2;
-- pg_subscription is a shared catalog, so count only this test's own
SELECT count(*) AS subscriptions_left
  FROM pg_subscription WHERE subname IN ('s1', 's2');
