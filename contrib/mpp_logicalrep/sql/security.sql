--
-- Who may see what, and who may drive it.
--
-- The source's connection string reaches every segment, because that is
-- where the readers connect from, and the SQL entry points drive gangs and
-- open sockets on a subscription's behalf.  Neither is for everybody.
--

CREATE ROLE mpp_lr_nobody;

-- ------------------------------------------------------------------
-- the connection string
-- ------------------------------------------------------------------

SET client_min_messages = error;
CREATE SUBSCRIPTION s_sec CONNECTION 'dbname=nowhere password=hunter2'
    PUBLICATION p WITH (slot_name = 'slot_sec', enabled = false);
RESET client_min_messages;

-- a superuser can read it
SELECT subconninfo LIKE '%hunter2%' AS superuser_sees_the_password
  FROM pg_subscription WHERE subname = 's_sec';

-- nobody else can, and that is the server's own column-level grant, not
-- something this module arranges
SET ROLE mpp_lr_nobody;
SELECT subconninfo FROM pg_subscription WHERE subname = 's_sec';
-- the rest of the row is readable, so this is the column and not the table
SELECT subname, subenabled FROM pg_subscription WHERE subname = 's_sec';
RESET ROLE;

-- ------------------------------------------------------------------
-- the SQL entry points
-- ------------------------------------------------------------------

-- They dispatch plans to reader and writer gangs, listen on a TCP port and
-- write into tables on a subscription's behalf.  Superuser only, checked
-- before anything else happens.
-- The refusal from a segment-executed one carries its host, port and pid, so
-- catch them all and report what matters: that they were refused, and for
-- the right reason.
SET ROLE mpp_lr_nobody;
DO $$
DECLARE
    stmt text;
BEGIN
    FOREACH stmt IN ARRAY ARRAY[
        'SELECT * FROM pg_catalog.gp_lr_connect(0, 0, '''')',
        'SELECT * FROM pg_catalog.gp_lr_drain(0, 0, 0, NULL, 0, NULL, NULL)',
        'SELECT * FROM pg_catalog.gp_lr_apply(0, 0, NULL, NULL, NULL)',
        'SELECT * FROM pg_catalog.gp_lr_copy_ports(''nosuchjob'')',
        'SELECT * FROM pg_catalog.gp_lr_copy_recv(0, ''nosuchjob'', 1) AS t(x int)'
    ] LOOP
        BEGIN
            EXECUTE stmt;
            RAISE NOTICE 'NOT refused: %', left(stmt, 40);
        EXCEPTION
            WHEN insufficient_privilege THEN
                RAISE NOTICE 'refused: %', left(stmt, 40);
            WHEN OTHERS THEN
                RAISE NOTICE 'refused for the wrong reason (%): %',
                             sqlstate, left(stmt, 40);
        END;
    END LOOP;
END $$;
RESET ROLE;

-- ------------------------------------------------------------------
-- the statistics views are not privileged: positions and counters only
-- ------------------------------------------------------------------

SET ROLE mpp_lr_nobody;
SELECT count(*) >= 0 AS readable FROM pg_catalog.gp_stat_mpp_subscription;
SELECT count(*) >= 0 AS readable FROM pg_catalog.gp_stat_mpp_subscription_segments;
RESET ROLE;

SET client_min_messages = error;
DROP SUBSCRIPTION s_sec;
RESET client_min_messages;
DROP ROLE mpp_lr_nobody;
