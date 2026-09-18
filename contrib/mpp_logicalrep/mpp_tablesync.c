/*-------------------------------------------------------------------------
 *
 * mpp_tablesync.c
 *	  Initial synchronisation of an MPP-apply subscription.
 *
 * Run by the apply driver (mpp_apply_worker.c) before it starts streaming.
 * The whole publication is synchronised as one unit under one cut point
 * (L_t, DS_t), taken on the source coordinator:
 *
 *   S1  slots       cluster-wide logical slots on the source (coordinator +
 *                   every segment, two_phase) if they do not exist yet
 *   S2  barrier     DS_pre := distributed snapshot; wait until the current
 *                   snapshot's xmin >= DS_pre.xmax, so every distributed
 *                   transaction in flight when the slots were created has
 *                   completed on all segments
 *   S3  cut point   BEGIN REPEATABLE READ on the source; in one statement
 *                   read L_t = pg_current_wal_lsn() and
 *                   DS_t = gp_current_distributed_snapshot()
 *   S4  copy        per table, inside that transaction: the target runs
 *                   INSERT INTO t SELECT * FROM gp_lr_copy_recv(...) and the
 *                   source runs COPY (SELECT ...) TO PROGRAM
 *                   'cbdb_lr_copyhelper send ...' ON SEGMENT; data
 *                   goes segment to segment, the target's Motion redistributes
 *   S5  catch-up    the driver streams from the slots and drops every gxid
 *                   visible under DS_t (already in the COPY); self-limiting
 *
 * State lives in the target's gp_subscription_rel:
 *   relid = 0 is the subscription-level marker: 'd' while a sync is in
 *   progress, 'r' when the streaming phase may run (sync_snapshot holds
 *   DS_t for restarts).  copy_data=false at CREATE SUBSCRIPTION writes the
 *   marker as 'r' at once, so no COPY ever happens.
 *   relid > 0 rows track each table: i -> d -> f -> r.  A table the source
 *   cannot decode (anything but heap) ends at 's' instead of 'r' when the
 *   subscription asked for unsupported_tables = 'snapshot': copied once, and
 *   never looked at again.
 *
 * Failure anywhere before the final commit leaves the marker at 'd'; the
 * next start truncates the listed tables and redoes the sync from S1 under
 * a fresh cut point.  The slots are reused if they exist.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_tablesync.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/value.h"
#include "mpp_apply_worker.h"
#include "storage/latch.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

int			gp_lr_slot_loss_action = LR_SLOT_LOSS_REINIT;

#define LR_SYNC_MARKER_RELID	0
#define LR_SYNC_BARRIER_TIMEOUT_MS	120000
#define LR_SYNC_PORTS_TIMEOUT_MS	120000
#define LR_SYNC_POLL_MS				100

/* one table of the publication */
typedef struct LrSyncRel
{
	Oid			relid;			/* target relation */
	char	   *qualified;		/* quoted "nsp"."rel" (same on both sides) */
	char	   *collist;		/* quoted column names, target order */
	char	   *coldefs;		/* column definition list for the SRF */
	bool		replicated;		/* the SOURCE table is DISTRIBUTED REPLICATED:
								 * every segment holds every row, so the
								 * segment-to-segment transport would send each
								 * row once per segment */
	bool		tracked;		/* false: the source table's changes are never
								 * decoded, so the copy is all there will ever
								 * be -- state 's' */
	bool		direct;			/* the two sides place this table's rows the
								 * same way, so a source segment's share is
								 * already the target segment's share and the
								 * copy can be loaded where it lands, without
								 * the redistribution the general path does */
} LrSyncRel;

/* where an incremental job stands between two calls of LrSyncAddStep() */
typedef enum LrSyncStep
{
	LR_SYNC_STEP_INIT,			/* nothing sent yet */
	LR_SYNC_STEP_PORTS,			/* target INSERT sent, ports not published */
	LR_SYNC_STEP_COPY,			/* source COPY sent, both sides running */
	LR_SYNC_STEP_DONE			/* every table copied */
} LrSyncStep;

typedef struct LrSync
{
	LrWorkerConfig *cfg;
	MemoryContext cxt;

	PGconn	   *tgt;			/* target: the INSERT ... SELECT side */
	bool		tgt_busy;		/* an INSERT is in flight on tgt */
	PGconn	   *tgt2;			/* target: control and polling */
	PGconn	   *src;			/* source coordinator, SQL */

	int			ntgtsegs;
	int			nsrcsegs;
	bool		crc;
	/*
	 * The general path spawns the helper from gp_lr_copy_recv(), which reads
	 * these on the segment it runs on.  The direct path spawns it from a COPY
	 * program string, which is one literal for the whole cluster, so they are
	 * read once from the target coordinator: a cluster that sets them per
	 * segment has to leave the direct path alone.
	 */
	char	   *bindaddr;
	int			port_lo;
	int			port_hi;
	bool		allow_direct;	/* mpp_logicalrep.direct_copy on the target */

	List	   *rels;			/* LrSyncRel* */
	LrUnsupportedTables unsupported;	/* what to do about a table whose
										 * changes the source cannot decode */

	/* cut point */
	char	   *lt;				/* L_t as text */
	int64		ds_xmax;
	List	   *ds_inprogress;	/* int64* */
	char	   *ds_text;		/* serialised DS_t */

	/* incremental job (LrSyncAddStart/Step/Finish), stepped by the driver */
	LrSyncStep	step;
	ListCell   *cur;			/* table being copied */
	char	   *cur_job;
	char	   *cur_targets;
	TimestampTz cur_started;
	bool		src_busy;		/* a COPY is in flight on src */
	int64		cur_src_rows;
} LrSync;

static PGresult *sync_exec(LrSync *s, PGconn *conn, const char *who,
						   const char *sql, int nparams, const char *const *params);
static void sync_connect(LrSync *s);
static void sync_disconnect(LrSync *s);
static char sync_marker_state(LrSync *s, char **snapshot);
static void sync_fetch_tables(LrSync *s);
static void sync_write_states(LrSync *s, char rel_state, bool truncate);
static void sync_ensure_slots(LrSync *s);
static void sync_barrier(LrSync *s);
static void sync_cut_point(LrSync *s);
static void sync_copy_table(LrSync *s, LrSyncRel *r);
static void copy_begin(LrSync *s, LrSyncRel *r);
static bool copy_ports_ready(LrSync *s, LrSyncRel *r);
static void copy_send(LrSync *s, LrSyncRel *r);
static bool copy_poll_done(LrSync *s, LrSyncRel *r);
static void sync_connect_source(LrSync *s);
static void sync_fetch_pending(LrSync *s);
static void sync_finish_rels(LrSync *s);
static void add_start_body(LrSync *s);
static void sync_fill_columns(LrSync *s, LrSyncRel *r);
static void sync_fill_source_info(LrSync *s, LrSyncRel *r);
static void sync_read_unsupported_mode(LrSync *s);
static void sync_report_unsupported(LrSync *s, LrSyncRel *r, const char *am);
static void sync_mark_untracked(LrSync *s);
static void sync_check_keys(LrSync *s, LrSyncRel *r);
static void copy_via_coordinators(LrSync *s, LrSyncRel *r);
static void sync_set_rel_state(LrSync *s, Oid relid, char state, const char *job);
static void sync_finish(LrSync *s);
static void sync_apply_snapshot(LrWorkerConfig *cfg, const char *text);
static int	sync_count_slots(LrSync *s);
static bool sync_handle_slot_loss(LrSync *s);
static void sync_wait(LrSync *s, int ms);

/* ------------------------------------------------------------------------
 * Entry point
 * ------------------------------------------------------------------------
 */

/*
 * Bring the subscription to the streaming phase.  Returns true when a sync
 * was performed, false when the marker already allowed streaming.  On both
 * paths cfg->sync_ds_* are filled from DS_t when there is one.
 */
bool
LrInitialSync(LrWorkerConfig *cfg)
{
	LrSync	   *s;
	MemoryContext cxt;
	MemoryContext oldcxt;
	char		marker;
	char	   *snapshot = NULL;
	ListCell   *lc;
	bool		did_sync = false;

	cxt = AllocSetContextCreate(CurrentMemoryContext, "LrSync", ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);
	s = palloc0(sizeof(LrSync));
	s->cfg = cfg;
	s->cxt = cxt;

	PG_TRY();
	{
		sync_connect(s);

		marker = sync_marker_state(s, &snapshot);

		/*
		 * Before streaming, make sure the slots we are about to stream from
		 * are still there (O1).  They are not part of a base backup and a
		 * segment failover loses them, in which case the whole subscription
		 * has to start over or stop.
		 */
		if (marker == 'r')
		{
			int			have = sync_count_slots(s);

			if (have == 0 && sync_handle_slot_loss(s))
			{
				marker = '\0';	/* redo everything below */
				snapshot = NULL;
			}
			else if (have != 0 && have != s->nsrcsegs + 1)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_IN_USE),
						 errmsg("slot \"%s\" is usable on only %d of %d source nodes",
								cfg->seg_slot, have, s->nsrcsegs + 1),
						 errdetail("The rest are missing, or were invalidated by the publisher because the WAL they asked for is gone."),
						 errhint("Drop it everywhere with gp_drop_replication_slot() and retry, or let the subscription resynchronise.")));
		}

		if (marker == 'r')
		{
			/* streaming phase: only the cut point matters, for the filter */
			if (snapshot)
				sync_apply_snapshot(cfg, snapshot);
		}
		else
		{
			bool		restart = (marker != '\0');

			elog(LOG, "mpp apply worker: subscription %u: %sinitial synchronisation",
				 cfg->subid, restart ? "redoing the interrupted " : "");

			sync_fetch_tables(s);
			sync_write_states(s, 'i', restart);
			sync_ensure_slots(s);
			sync_barrier(s);
			sync_cut_point(s);
			foreach(lc, s->rels)
				sync_copy_table(s, (LrSyncRel *) lfirst(lc));
			PQclear(sync_exec(s, s->src, "source", "COMMIT", 0, NULL));
			sync_finish(s);
			sync_apply_snapshot(cfg, s->ds_text);
			did_sync = true;

			elog(LOG, "mpp apply worker: subscription %u: initial synchronisation done: %d tables, L_t %s, DS_t xmax " INT64_FORMAT ", %d in progress",
				 cfg->subid, list_length(s->rels), s->lt, s->ds_xmax,
				 list_length(s->ds_inprogress));
		}
	}
	PG_CATCH();
	{
		sync_disconnect(s);
		MemoryContextSwitchTo(oldcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	sync_disconnect(s);
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);
	return did_sync;
}

/* ------------------------------------------------------------------------
 * Connections
 * ------------------------------------------------------------------------
 */

static PGresult *
sync_exec(LrSync *s, PGconn *conn, const char *who, const char *sql,
		  int nparams, const char *const *params)
{
	PGresult   *res;

	res = nparams > 0 ?
		PQexecParams(conn, sql, nparams, NULL, params, NULL, NULL, 0) :
		PQexec(conn, sql);
	if (res == NULL ||
		(PQresultStatus(res) != PGRES_TUPLES_OK &&
		 PQresultStatus(res) != PGRES_COMMAND_OK))
	{
		char	   *msg = pstrdup(res ? PQresultErrorMessage(res) : PQerrorMessage(conn));
		const char *state = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : NULL;
		int			code = ERRCODE_CONNECTION_EXCEPTION;

		/*
		 * Carry the far side's own SQLSTATE over: every statement of the
		 * synchronisation comes back through here, and calling all of them
		 * connection failures would tell run_initial_sync() that every one of
		 * them can pass on its own (O6).  Only a failure with no result at
		 * all really is one.
		 */
		if (state != NULL && strlen(state) == 5)
			code = MAKE_SQLSTATE(state[0], state[1], state[2], state[3], state[4]);

		if (res)
			PQclear(res);
		ereport(ERROR,
				(errcode(code),
				 errmsg("initial sync, %s: %s", who, msg),
				 errcontext("statement: %.300s", sql)));
	}
	return res;
}

static PGconn *
sync_connect_one(const char *conninfo, const char *what, const char *options)
{
	StringInfoData ci;
	PGconn	   *conn;

	initStringInfo(&ci);
	appendStringInfo(&ci, "%s application_name=mpp_apply_sync%s", conninfo,
					 options ? options : "");
	conn = PQconnectdb(ci.data);
	pfree(ci.data);
	if (PQstatus(conn) != CONNECTION_OK)
	{
		char	   *msg = pstrdup(PQerrorMessage(conn));

		PQfinish(conn);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("initial sync: could not connect to the %s: %s", what, msg)));
	}
	return conn;
}

static void
sync_connect(LrSync *s)
{
	PGresult   *res;

	s->tgt2 = sync_connect_one(s->cfg->target_conninfo, "target coordinator", NULL);

	res = sync_exec(s, s->tgt2, "target",
					"SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0",
					0, NULL);
	s->ntgtsegs = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	res = sync_exec(s, s->tgt2, "target", "SHOW mpp_logicalrep.copy_crc", 0, NULL);
	s->crc = (strcmp(PQgetvalue(res, 0, 0), "on") == 0);
	PQclear(res);
	res = sync_exec(s, s->tgt2, "target",
					"SELECT current_setting('mpp_logicalrep.copy_bind_address'),\n"
					"       current_setting('mpp_logicalrep.copy_port_lo'),\n"
					"       current_setting('mpp_logicalrep.copy_port_hi')",
					0, NULL);
	s->bindaddr = pstrdup(PQgetvalue(res, 0, 0));
	s->port_lo = atoi(PQgetvalue(res, 0, 1));
	s->port_hi = atoi(PQgetvalue(res, 0, 2));
	PQclear(res);
	res = sync_exec(s, s->tgt2, "target", "SHOW mpp_logicalrep.direct_copy", 0, NULL);
	s->allow_direct = (strcmp(PQgetvalue(res, 0, 0), "on") == 0);
	PQclear(res);
}

/*
 * How a side places this table's rows, as one string: the policy, then the
 * distribution key as column name and operator class in key order.  Two sides
 * that answer the same place every row the same way -- the operator class is
 * in it because that is what tells the legacy hash from the jump-consistent
 * one, and a table keyed on the same column with the other hash lands its rows
 * somewhere else entirely.  A randomly distributed table answers with an empty
 * key, which is the honest answer: any row may live anywhere, on both sides.
 */
static char *
sync_placement(LrSync *s, PGconn *conn, const char *who, const char *qualified)
{
	const char *params[1] = {qualified};
	PGresult   *res;
	char	   *sig;

	res = sync_exec(s, conn, who,
					"SELECT cast(p.policytype as text) || '|' || coalesce(\n"
					"        (SELECT string_agg(a.attname || ':' || n.nspname || '.' || o.opcname,\n"
					"                           ',' ORDER BY k.ord)\n"
					"           FROM unnest(cast(p.distkey as int2[])) WITH ORDINALITY AS k(attnum, ord)\n"
					"           JOIN pg_attribute a ON a.attrelid = p.localoid AND a.attnum = k.attnum\n"
					"           JOIN pg_opclass o ON o.oid = p.distclass[k.ord - 1]\n"
					"           JOIN pg_namespace n ON n.oid = o.opcnamespace), '')\n"
					"  FROM gp_distribution_policy p\n"
					" WHERE p.localoid = to_regclass($1)",
					1, params);
	sig = (PQntuples(res) == 1 && !PQgetisnull(res, 0, 0))
		? pstrdup(PQgetvalue(res, 0, 0)) : NULL;
	PQclear(res);
	return sig;
}

/*
 * Stop an in-flight receiving INSERT before closing its connection.  Just
 * closing would leave the QD backend blocked in gp_lr_copy_recv() (the helper
 * waits for senders that will never come) holding the table's lock until the
 * helper's accept timeout; a redo would then wait on it in TRUNCATE.
 */
static void
sync_cancel_tgt(LrSync *s)
{
	PGcancel   *cancel;
	char		errbuf[256];
	PGresult   *r;

	if (s->tgt == NULL || !s->tgt_busy)
		return;
	if ((cancel = PQgetCancel(s->tgt)) != NULL)
	{
		(void) PQcancel(cancel, errbuf, sizeof(errbuf));
		PQfreeCancel(cancel);
	}
	while ((r = PQgetResult(s->tgt)) != NULL)
		PQclear(r);
	s->tgt_busy = false;
}

static void
sync_disconnect(LrSync *s)
{
	sync_cancel_tgt(s);
	if (s->tgt)
		PQfinish(s->tgt);
	if (s->tgt2)
		PQfinish(s->tgt2);
	if (s->src)
		PQfinish(s->src);
	s->tgt = s->tgt2 = s->src = NULL;
}

/* a latch wait that honours the worker's stop request */
static void
sync_wait(LrSync *s, int ms)
{
	CHECK_FOR_INTERRUPTS();
	if (s->cfg->should_stop && s->cfg->should_stop(s->cfg->stop_arg))
		ereport(ERROR,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("initial sync interrupted: the subscription changed")));
	(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 ms, WAIT_EVENT_LOGICAL_SYNC_DATA);
	ResetLatch(MyLatch);
}

/* ------------------------------------------------------------------------
 * State rows
 * ------------------------------------------------------------------------
 */

/* '\0' = no marker row */
static char
sync_marker_state(LrSync *s, char **snapshot)
{
	PGresult   *res;
	char		subid[16];
	const char *params[1] = {subid};
	char		state = '\0';

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	res = sync_exec(s, s->tgt2, "target",
					"SELECT state, sync_snapshot FROM pg_catalog.gp_subscription_rel "
					"WHERE subid = $1 AND relid = 0",
					1, params);
	if (PQntuples(res) == 1)
	{
		state = PQgetvalue(res, 0, 0)[0];
		if (!PQgetisnull(res, 0, 1))
			*snapshot = pstrdup(PQgetvalue(res, 0, 1));
	}
	PQclear(res);
	return state;
}

/* the source coordinator connection, and how many segments it has */
static void
sync_connect_source(LrSync *s)
{
	PGresult   *res;

	if (s->src != NULL)
		return;
	s->src = sync_connect_one(s->cfg->source_conninfo, "source coordinator", NULL);
	res = sync_exec(s, s->src, "source",
					"SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0",
					0, NULL);
	s->nsrcsegs = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
}

/* target-side column list and definition list of one relation */
static void
sync_fill_columns(LrSync *s, LrSyncRel *r)
{
	const char *params[1] = {r->qualified};
	PGresult   *res;

	res = sync_exec(s, s->tgt2, "target",
					"SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),\n"
					"       string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)\n"
					"  FROM pg_attribute a\n"
					" WHERE a.attrelid = to_regclass($1) AND a.attnum > 0\n"
					"   AND NOT a.attisdropped AND a.attgenerated = ''",
					1, params);
	if (PQntuples(res) != 1 || PQgetisnull(res, 0, 0))
	{
		PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table synchronisation: target relation %s does not exist", r->qualified),
				 errhint("DDL is not replicated; create the table on the target first.")));
	}
	r->collist = pstrdup(PQgetvalue(res, 0, 0));
	r->coldefs = pstrdup(PQgetvalue(res, 0, 1));
	PQclear(res);
}

/*
 * The two shapes that cannot be applied, reported while there is still time
 * to change them.  Neither is fatal: they only bite the
 * UPDATEs described below, and a table that is only inserted into or deleted
 * from is fine either way, so this warns instead of refusing.
 */
typedef struct LrRelKeys
{
	char		policytype;		/* 'p' hash or random, 'r' replicated */
	char		replident;		/* pg_class.relreplident */
	List	   *distkey;		/* char*: distribution key columns */
	List	   *identity;		/* char*: replica identity columns */
} LrRelKeys;

static List *
name_list(const char *csv)
{
	List	   *result = NIL;
	char	   *copy;
	char	   *save = NULL;
	char	   *tok;

	if (csv == NULL || *csv == '\0')
		return NIL;
	copy = pstrdup(csv);
	for (tok = strtok_r(copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
		result = lappend(result, pstrdup(tok));
	return result;
}

static bool
names_subset(List *sub, List *super)
{
	ListCell   *lc;

	foreach(lc, sub)
	{
		ListCell   *sc;
		bool		found = false;

		foreach(sc, super)
			if (strcmp((char *) lfirst(lc), (char *) lfirst(sc)) == 0)
				found = true;
		if (!found)
			return false;
	}
	return true;
}

static char *
names_text(List *names)
{
	StringInfoData buf;
	ListCell   *lc;

	initStringInfo(&buf);
	foreach(lc, names)
		appendStringInfo(&buf, "%s%s", buf.len ? ", " : "", (char *) lfirst(lc));
	return buf.data;
}

/* one side's policy and keys, by name */
static void
sync_fetch_keys(LrSync *s, PGconn *conn, const char *who, const char *qualified,
				LrRelKeys *keys)
{
	const char *params[1] = {qualified};
	PGresult   *res;

	memset(keys, 0, sizeof(*keys));
	res = sync_exec(s, conn, who,
					"SELECT coalesce(p.policytype::text, ''), c.relreplident::text,\n"
					"       (SELECT coalesce(string_agg(a.attname, ','), '')\n"
					"          FROM pg_attribute a\n"
					"         WHERE a.attrelid = c.oid AND a.attnum = ANY (p.distkey::int2[])),\n"
					"       (SELECT coalesce(string_agg(a.attname, ','), '')\n"
					"          FROM pg_index i\n"
					"          JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY (i.indkey::int2[])\n"
					"         WHERE i.indrelid = c.oid\n"
					"           AND ((c.relreplident = 'd' AND i.indisprimary)\n"
					"             OR (c.relreplident = 'i' AND i.indisreplident)))\n"
					"  FROM pg_class c LEFT JOIN gp_distribution_policy p ON p.localoid = c.oid\n"
					" WHERE c.oid = to_regclass($1)",
					1, params);
	if (PQntuples(res) == 1)
	{
		keys->policytype = PQgetvalue(res, 0, 0)[0];
		keys->replident = PQgetvalue(res, 0, 1)[0];
		keys->distkey = name_list(PQgetvalue(res, 0, 2));
		keys->identity = name_list(PQgetvalue(res, 0, 3));
	}
	PQclear(res);
}

static void
sync_check_keys(LrSync *s, LrSyncRel *r)
{
	LrRelKeys	src,
				tgt;

	sync_connect_source(s);
	sync_fetch_keys(s, s->src, "source", r->qualified, &src);
	sync_fetch_keys(s, s->tgt2, "target", r->qualified, &tgt);

	/*
	 * Applying an UPDATE needs to know which target segment holds the row,
	 * which means the old value of the target's distribution key.  The
	 * source only sends old values of its replica identity.
	 */
	if (tgt.policytype == 'p' && tgt.distkey != NIL &&
		src.replident != REPLICA_IDENTITY_FULL &&
		!names_subset(tgt.distkey, src.identity))
		ereport(WARNING,
				(errmsg("logical replication: the distribution key (%s) of target relation %s is not part of the replica identity of the source relation",
						names_text(tgt.distkey), r->qualified),
				 errdetail("An UPDATE that changes it moves the row to another target segment, which needs the old value of the key; the source does not send it. Such an UPDATE will be refused."),
				 errhint("Set REPLICA IDENTITY FULL on the source relation, or distribute the target relation by columns of its replica identity. INSERT and DELETE are not affected.")));

	/*
	 * An UPDATE of the source's distribution key moves the row between
	 * source segments, so it reaches the target as a DELETE from one stream
	 * and an INSERT from another.  The target must be able to tell those two
	 * rows apart, or it sees one row deleted and inserted in an order nobody
	 * defines.
	 */
	if (src.policytype == 'p' && src.distkey != NIL &&
		tgt.replident != REPLICA_IDENTITY_FULL &&
		!names_subset(src.distkey, tgt.identity))
		ereport(WARNING,
				(errmsg("logical replication: the distribution key (%s) of source relation %s is not part of the replica identity of the target relation",
						names_text(src.distkey), r->qualified),
				 errdetail("Updating it on the source moves the row between source segments, which reaches the target as a DELETE and an INSERT of what it considers the same row, in no defined order."),
				 errhint("Include those columns in the target's replica identity, or distribute both sides by the same columns. Changes that leave the source's distribution key alone are not affected.")));
}

/*
 * What this subscription wants done about a table the source cannot decode.
 * Read from the module's own registry rather than carried in the worker's
 * config, so that ALTER SUBSCRIPTION ... SET is in force for
 * the next table list, added tables included.
 */
static void
sync_read_unsupported_mode(LrSync *s)
{
	char		subid[16];
	const char *params[1] = {subid};
	PGresult   *res;

	s->unsupported = LR_UNSUPPORTED_ERROR;
	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	res = sync_exec(s, s->tgt2, "target",
					"SELECT unsupported_tables FROM pg_catalog.gp_mpp_subscription"
					" WHERE subid = $1",
					1, params);
	/* no row: not one of ours to begin with, so the strict default stands */
	if (PQntuples(res) == 1)
		(void) LrUnsupportedTablesParse(PQgetvalue(res, 0, 0), &s->unsupported);
	PQclear(res);
}

/*
 * The source table is not a heap table, so nothing it does will ever reach
 * the stream: the publisher has no decoding routine for it.  Say so while
 * the publication can still be changed -- the alternative is a table that
 * looks replicated, never errors, and quietly stops being true after the
 * initial copy.
 */
static void
sync_report_unsupported(LrSync *s, LrSyncRel *r, const char *am)
{
	if (s->unsupported == LR_UNSUPPORTED_ERROR)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("logical replication: source relation %s uses access method \"%s\", whose changes are not decoded",
						r->qualified, am),
				 errdetail("Only heap tables produce logical decoding output on the publisher. This table would be copied once and then never change again, with no error to show for it."),
				 errhint("Leave it out of the publication, or accept the one-time copy with ALTER SUBSCRIPTION %s SET (unsupported_tables = 'snapshot').",
						 quote_identifier(s->cfg->subname ? s->cfg->subname : "?"))));

	ereport(WARNING,
			(errmsg("logical replication: source relation %s uses access method \"%s\", whose changes are not decoded",
					r->qualified, am),
			 errdetail("It is copied once, as of this subscription's cut point, and then left alone: changes made on the publisher afterwards never reach this target."),
			 errhint("gp_subscription_rel records it in state 's'.")));
	r->tracked = false;
}

/*
 * The source side of one table: is it DISTRIBUTED REPLICATED (which decides
 * how it is copied), and can its changes be decoded at all?
 */
static void
sync_fill_source_info(LrSync *s, LrSyncRel *r)
{
	const char *params[1] = {r->qualified};
	PGresult   *res;

	sync_connect_source(s);		/* the pending-table path has not yet */
	r->tracked = true;
	res = sync_exec(s, s->src, "source",
					"SELECT coalesce(p.policytype::text, ''), coalesce(am.amname, '')\n"
					"  FROM pg_class c\n"
					"  LEFT JOIN gp_distribution_policy p ON p.localoid = c.oid\n"
					"  LEFT JOIN pg_am am ON am.oid = c.relam\n"
					" WHERE c.oid = to_regclass($1)",
					1, params);
	if (PQntuples(res) == 1)
	{
		char	   *am = pstrdup(PQgetvalue(res, 0, 1));

		r->replicated = (PQgetvalue(res, 0, 0)[0] == 'r');
		if (strcmp(am, "heap") != 0)
			sync_report_unsupported(s, r, *am ? am : "none");
	}
	PQclear(res);

	/*
	 * May this table be loaded where it lands?  Only if both sides have the
	 * same number of segments and place its rows the same way.  A replicated
	 * table never can: every segment holds every row, and it has a copy path
	 * of its own.
	 */
	r->direct = false;
	if (s->allow_direct && !r->replicated && s->nsrcsegs == s->ntgtsegs)
	{
		char	   *src_sig = sync_placement(s, s->src, "source", r->qualified);
		char	   *tgt_sig = sync_placement(s, s->tgt2, "target", r->qualified);

		r->direct = (src_sig != NULL && tgt_sig != NULL &&
					 strcmp(src_sig, tgt_sig) == 0);
		if (!r->direct)
			elog(DEBUG1, "mpp apply worker: subscription %u: %s is copied through a redistribution: source places it as %s, target as %s, over %d and %d segments",
				 s->cfg->subid, r->qualified,
				 src_sig ? src_sig : "?", tgt_sig ? tgt_sig : "?",
				 s->nsrcsegs, s->ntgtsegs);
	}
}

/* the untracked tables of this job -> 's' (they are done, not ready) */
static void
sync_mark_untracked(LrSync *s)
{
	char		subid[16];
	StringInfoData oids;
	const char *params[2] = {subid, NULL};
	ListCell   *lc;

	initStringInfo(&oids);
	foreach(lc, s->rels)
	{
		LrSyncRel  *r = (LrSyncRel *) lfirst(lc);

		if (!r->tracked)
			appendStringInfo(&oids, "%s%u", oids.len ? "," : "{", r->relid);
	}
	if (oids.len == 0)
	{
		pfree(oids.data);
		return;
	}
	appendStringInfoChar(&oids, '}');

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	params[1] = oids.data;
	PQclear(sync_exec(s, s->tgt2, "target",
					  "UPDATE pg_catalog.gp_subscription_rel SET state = 's'"
					  " WHERE subid = $1 AND relid = ANY ($2::oid[])",
					  2, params));
	pfree(oids.data);
}

/*
 * The two spellings of unsupported_tables, shared with the utility hook.
 */
bool
LrUnsupportedTablesParse(const char *value, LrUnsupportedTables *mode)
{
	if (value == NULL)
		return false;
	if (pg_strcasecmp(value, "error") == 0)
	{
		*mode = LR_UNSUPPORTED_ERROR;
		return true;
	}
	if (pg_strcasecmp(value, "snapshot") == 0)
	{
		*mode = LR_UNSUPPORTED_SNAPSHOT;
		return true;
	}
	return false;
}

const char *
LrUnsupportedTablesName(LrUnsupportedTables mode)
{
	return mode == LR_UNSUPPORTED_SNAPSHOT ? "snapshot" : "error";
}

/*
 * The copy of a replicated table: every segment holds every
 * row, so the rows are read once on the source coordinator and written once
 * on the target coordinator, which broadcasts them again.  Both directions
 * of a replicated table are small by construction, so the coordinators are
 * not a bottleneck here -- and this is also the only shape the
 * segment-to-segment transport cannot express.
 */
static void
copy_via_coordinators(LrSync *s, LrSyncRel *r)
{
	StringInfoData sql;
	PGresult   *res;
	int64		nrows = 0;
	char	   *buf = NULL;
	int			n;
	char	   *msg = NULL;

	sync_set_rel_state(s, r->relid, 'd', NULL);
	if (s->tgt == NULL)
		s->tgt = sync_connect_one(s->cfg->target_conninfo, "target coordinator",
								  " options='-c client_connection_check_interval=1000'");

	initStringInfo(&sql);
	appendStringInfo(&sql, "COPY (SELECT %s FROM %s) TO STDOUT", r->collist, r->qualified);
	res = PQexec(s->src, sql.data);
	if (res == NULL || PQresultStatus(res) != PGRES_COPY_OUT)
	{
		msg = pstrdup(res ? PQresultErrorMessage(res) : PQerrorMessage(s->src));
		if (res)
			PQclear(res);
		pfree(sql.data);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, source COPY of replicated %s: %s", r->qualified, msg)));
	}
	PQclear(res);

	resetStringInfo(&sql);
	appendStringInfo(&sql, "COPY %s (%s) FROM STDIN", r->qualified, r->collist);
	res = PQexec(s->tgt, sql.data);
	if (res == NULL || PQresultStatus(res) != PGRES_COPY_IN)
	{
		msg = pstrdup(res ? PQresultErrorMessage(res) : PQerrorMessage(s->tgt));
		if (res)
			PQclear(res);
		pfree(sql.data);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, target COPY of replicated %s: %s", r->qualified, msg)));
	}
	PQclear(res);
	pfree(sql.data);
	s->tgt_busy = true;

	while ((n = PQgetCopyData(s->src, &buf, 0)) > 0)
	{
		if (PQputCopyData(s->tgt, buf, n) != 1)
		{
			PQfreemem(buf);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("initial sync, target COPY of replicated %s: %s",
							r->qualified, PQerrorMessage(s->tgt))));
		}
		PQfreemem(buf);
		buf = NULL;
		nrows++;
		CHECK_FOR_INTERRUPTS();
	}
	if (n == -2)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, source COPY of replicated %s: %s",
						r->qualified, PQerrorMessage(s->src))));
	while ((res = PQgetResult(s->src)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_COMMAND_OK && msg == NULL)
			msg = pstrdup(PQresultErrorMessage(res));
		PQclear(res);
	}
	if (msg)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, source COPY of replicated %s: %s", r->qualified, msg)));

	if (PQputCopyEnd(s->tgt, NULL) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, target COPY of replicated %s: %s",
						r->qualified, PQerrorMessage(s->tgt))));
	{
		int64		tgt_rows = -1;

		while ((res = PQgetResult(s->tgt)) != NULL)
		{
			if (PQresultStatus(res) == PGRES_COMMAND_OK)
				tgt_rows = strtoll(PQcmdTuples(res), NULL, 10);
			else if (msg == NULL)
				msg = pstrdup(PQresultErrorMessage(res));
			PQclear(res);
		}
		s->tgt_busy = false;
		if (msg)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("initial sync, target COPY of replicated %s: %s", r->qualified, msg)));
		if (tgt_rows != nrows)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("initial sync: %s: source sent " INT64_FORMAT " rows, target received " INT64_FORMAT,
							r->qualified, nrows, tgt_rows)));
	}

	sync_set_rel_state(s, r->relid, 'f', NULL);
	elog(LOG, "mpp apply worker: subscription %u: copied replicated %s: " INT64_FORMAT " rows (through the coordinators)",
		 s->cfg->subid, r->qualified, nrows);
}

/*
 * The tables of this subscription that are not ready yet (state 'i', 'd'
 * or 'f'; 'r' is streaming and 's' is a copy nobody tracks):
 * added by ALTER SUBSCRIPTION ... REFRESH PUBLICATION ('i'), or left behind
 * by an interrupted incremental job ('d'/'f').  Both are (re)copied under a
 * fresh cut point, so the rows of an interrupted copy are truncated first.
 */
static void
sync_fetch_pending(LrSync *s)
{
	char		subid[16];
	const char *params[1] = {subid};
	PGresult   *res;

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	sync_read_unsupported_mode(s);
	res = sync_exec(s, s->tgt2, "target",
					"SELECT r.relid, quote_ident(n.nspname) || '.' || quote_ident(c.relname)\n"
					"  FROM pg_catalog.gp_subscription_rel r\n"
					"  JOIN pg_class c ON c.oid = r.relid\n"
					"  JOIN pg_namespace n ON n.oid = c.relnamespace\n"
					" WHERE r.subid = $1 AND r.relid <> 0 AND r.state NOT IN ('r', 's')\n"
					" ORDER BY 2",
					1, params);
	for (int i = 0; i < PQntuples(res); i++)
	{
		LrSyncRel  *r = palloc0(sizeof(LrSyncRel));

		r->relid = (Oid) strtoul(PQgetvalue(res, i, 0), NULL, 10);
		r->qualified = pstrdup(PQgetvalue(res, i, 1));
		sync_fill_columns(s, r);
		sync_fill_source_info(s, r);
		if (r->tracked)
			sync_check_keys(s, r);
		s->rels = lappend(s->rels, r);
	}
	PQclear(res);
}

/*
 * The publication's tables from the source, resolved to target relations
 * with their column lists (target order, matched by name).
 */
static void
sync_fetch_tables(LrSync *s)
{
	StringInfoData sql;
	StringInfoData pubs;
	ListCell   *lc;
	PGresult   *res;

	sync_connect_source(s);
	sync_read_unsupported_mode(s);

	initStringInfo(&pubs);
	foreach(lc, s->cfg->publications)
	{
		const char *name = strVal(lfirst(lc));
		char	   *lit = PQescapeLiteral(s->src, name, strlen(name));

		appendStringInfo(&pubs, "%s%s", pubs.len ? ", " : "", lit);
		PQfreemem(lit);
	}

	initStringInfo(&sql);
	appendStringInfo(&sql,
					 "SELECT DISTINCT quote_ident(n.nspname) || '.' || quote_ident(c.relname)\n"
					 "  FROM pg_class c\n"
					 "  JOIN pg_namespace n ON n.oid = c.relnamespace\n"
					 "  JOIN (SELECT (pg_get_publication_tables(VARIADIC array_agg(pubname::text))).*\n"
					 "          FROM pg_publication WHERE pubname IN (%s)) AS gpt\n"
					 "    ON gpt.relid = c.oid\n"
					 " ORDER BY 1", pubs.data);
	res = sync_exec(s, s->src, "source", sql.data, 0, NULL);

	for (int i = 0; i < PQntuples(res); i++)
	{
		LrSyncRel  *r = palloc0(sizeof(LrSyncRel));
		const char *params[1];
		PGresult   *tres;

		r->qualified = pstrdup(PQgetvalue(res, i, 0));
		params[0] = r->qualified;
		tres = sync_exec(s, s->tgt2, "target",
						 "SELECT c.oid,\n"
						 "       string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum),\n"
						 "       string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)\n"
						 "  FROM pg_class c JOIN pg_attribute a ON a.attrelid = c.oid\n"
						 " WHERE c.oid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''\n"
						 " GROUP BY c.oid",
						 1, params);
		if (PQntuples(tres) != 1)
		{
			PQclear(tres);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("initial sync: target relation %s does not exist", r->qualified),
					 errhint("DDL is not replicated; create the table on the target first.")));
		}
		r->relid = (Oid) strtoul(PQgetvalue(tres, 0, 0), NULL, 10);
		r->collist = pstrdup(PQgetvalue(tres, 0, 1));
		r->coldefs = pstrdup(PQgetvalue(tres, 0, 2));
		PQclear(tres);
		sync_fill_source_info(s, r);
		if (r->tracked)
			sync_check_keys(s, r);
		s->rels = lappend(s->rels, r);
	}
	PQclear(res);
	pfree(sql.data);
	pfree(pubs.data);
}

/*
 * marker -> 'd', every table -> rel_state (rows created as needed).  When
 * redoing an interrupted sync the listed tables are truncated first, so the
 * new COPY lands in empty tables.
 */
static void
sync_write_states(LrSync *s, char rel_state, bool truncate)
{
	ListCell   *lc;
	char		subid[16];
	char		relid[16];
	char		state[2] = {rel_state, '\0'};
	const char *params[3] = {subid, relid, state};

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);

	PQclear(sync_exec(s, s->tgt2, "target", "BEGIN", 0, NULL));
	if (truncate)
	{
		foreach(lc, s->rels)
		{
			LrSyncRel  *r = (LrSyncRel *) lfirst(lc);
			char	   *sql = psprintf("TRUNCATE TABLE %s", r->qualified);

			PQclear(sync_exec(s, s->tgt2, "target", sql, 0, NULL));
			pfree(sql);
		}
	}
	/* marker */
	snprintf(relid, sizeof(relid), "%d", LR_SYNC_MARKER_RELID);
	state[0] = 'd';
	PQclear(sync_exec(s, s->tgt2, "target",
					  "INSERT INTO pg_catalog.gp_subscription_rel (subid, relid, state) VALUES ($1, $2, $3) "
					  "ON CONFLICT (subid, relid) DO UPDATE SET state = EXCLUDED.state, "
					  "sync_lsn = NULL, sync_snapshot = NULL, copy_job = NULL",
					  3, params));
	/* tables */
	state[0] = rel_state;
	foreach(lc, s->rels)
	{
		LrSyncRel  *r = (LrSyncRel *) lfirst(lc);

		snprintf(relid, sizeof(relid), "%u", r->relid);
		PQclear(sync_exec(s, s->tgt2, "target",
						  "INSERT INTO pg_catalog.gp_subscription_rel (subid, relid, state) VALUES ($1, $2, $3) "
						  "ON CONFLICT (subid, relid) DO UPDATE SET state = EXCLUDED.state, "
						  "sync_lsn = NULL, sync_snapshot = NULL, copy_job = NULL",
						  3, params));
	}
	PQclear(sync_exec(s, s->tgt2, "target", "COMMIT", 0, NULL));
}

static void
sync_set_rel_state(LrSync *s, Oid relid, char state, const char *job)
{
	char		subid[16];
	char		relidbuf[16];
	char		statebuf[2] = {state, '\0'};
	const char *params[4] = {subid, relidbuf, statebuf, job};

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	snprintf(relidbuf, sizeof(relidbuf), "%u", relid);
	PQclear(sync_exec(s, s->tgt2, "target",
					  "UPDATE pg_catalog.gp_subscription_rel SET state = $3, copy_job = $4 "
					  "WHERE subid = $1 AND relid = $2",
					  4, params));
}

/* everything -> 'r' with the cut point, in one transaction */
static void
sync_finish(LrSync *s)
{
	char		subid[16];
	const char *params[3] = {subid, s->lt, s->ds_text};

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	PQclear(sync_exec(s, s->tgt2, "target", "BEGIN", 0, NULL));
	PQclear(sync_exec(s, s->tgt2, "target",
					  "UPDATE pg_catalog.gp_subscription_rel SET state = 'r', sync_lsn = $2::pg_lsn, "
					  "sync_snapshot = $3, copy_job = NULL WHERE subid = $1",
					  3, params));
	sync_mark_untracked(s);
	PQclear(sync_exec(s, s->tgt2, "target", "COMMIT", 0, NULL));
}

/* ------------------------------------------------------------------------
 * S1 slots, S2 barrier, S3 cut point
 * ------------------------------------------------------------------------
 */

/* how many source nodes still have this subscription's slot */
static int
sync_count_slots(LrSync *s)
{
	PGresult   *res;
	const char *params[1] = {s->cfg->seg_slot};
	int			have;

	sync_connect_source(s);

	/*
	 * A slot that is still listed but can no longer be streamed from counts
	 * as gone, because that is what it is: the publisher invalidates a slot
	 * when the WAL it asked for has been recycled (or, on a standby, when it
	 * conflicts with recovery), clears its restart_lsn and refuses every
	 * START_REPLICATION afterwards.  Counting the row would let the caller
	 * walk into that refusal instead of deciding what to do about it, and the
	 * refusal happens outside this function -- in the reader gang and in the
	 * driver's own connection, where nothing grades it and the worker simply
	 * exits to be started again.
	 */
	res = sync_exec(s, s->src, "source",
					"SELECT count(*) FROM gp_replication_slots\n"
					" WHERE slot_name = $1\n"
					"   AND restart_lsn IS NOT NULL\n"
					"   AND wal_status IS DISTINCT FROM 'lost'\n"
					"   AND NOT coalesce(conflicting, false)",
					1, params);
	have = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	return have;
}

/*
 * The streaming phase is about to start but the source's slots are no longer
 * usable.  Two ways that happens, and they need the same answer:
 *
 *   gone         a segment failover takes its slots with it, since a logical
 *                slot is local to its primary and PG16 has no slot
 *                synchronisation
 *   invalidated  the publisher recycled the WAL the slot asked for, cleared
 *                its restart_lsn and will refuse to stream from it
 *
 * Either way everything between that position and now is unrecoverable, so
 * the only consistent answers are "redo the subscription from scratch" or
 * "stop and tell someone".  Returns true when the caller should redo the
 * initial synchronisation.
 */
static bool
sync_handle_slot_loss(LrSync *s)
{
	if (gp_lr_slot_loss_action == LR_SLOT_LOSS_DISABLE)
	{
		{
			char	   *sql = psprintf("UPDATE pg_catalog.gp_mpp_subscription "
									   "SET enabled = false, disable_reason = "
									   "'the source replication slots are gone or invalidated' "
									   "WHERE subid = %u", s->cfg->subid);

			PQclear(sync_exec(s, s->tgt2, "target", sql, 0, NULL));
			pfree(sql);
		}
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("replication slot \"%s\" can no longer be streamed from; subscription %u disabled",
						s->cfg->seg_slot, s->cfg->subid),
				 errdetail("The slots are either gone -- they are local to the source primaries and are lost on failover -- or invalidated, which the publisher does when the WAL they asked for has been recycled."),
				 errhint("Re-enable it (ALTER SUBSCRIPTION ... ENABLE) to resynchronise from scratch, or set mpp_logicalrep.slot_loss_action=reinit. To stop it happening again, keep the subscription running, or cap the publisher's retention with max_slot_wal_keep_size and watch pg_replication_slots.wal_status.")));
	}

	ereport(WARNING,
			(errmsg("replication slot \"%s\" can no longer be streamed from; redoing the initial synchronisation of subscription %u",
					s->cfg->seg_slot, s->cfg->subid),
			 errdetail("The slot is gone -- a logical slot is local to its primary and is lost when the segment fails over -- or invalidated because the WAL it asked for has been recycled. Either way the changes since then cannot be recovered from it.")));

	/* forget every trace of the old run; the sync below rebuilds it */
	{
		char		subid[16];
		const char *params[1] = {subid};

		snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
		PQclear(sync_exec(s, s->tgt2, "target", "BEGIN", 0, NULL));
		PQclear(sync_exec(s, s->tgt2, "target",
						  "DELETE FROM pg_catalog.gp_subscription_applied WHERE subid = $1", 1, params));
		PQclear(sync_exec(s, s->tgt2, "target",
						  "DELETE FROM pg_catalog.gp_subscription_progress WHERE subid = $1", 1, params));
		PQclear(sync_exec(s, s->tgt2, "target",
						  "DELETE FROM pg_catalog.gp_subscription_rel WHERE subid = $1", 1, params));
		PQclear(sync_exec(s, s->tgt2, "target", "COMMIT", 0, NULL));
	}
	s->cfg->sync_ds_xmax = 0;
	s->cfg->nsync_ds_inprogress = 0;
	s->cfg->sync_ds_inprogress = NULL;
	return true;
}

static void
sync_ensure_slots(LrSync *s)
{
	const char *params[1] = {s->cfg->seg_slot};
	PGresult   *res;
	int			have;

	have = sync_count_slots(s);
	if (have == s->nsrcsegs + 1)
		return;					/* coordinator + every segment */
	if (have != 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("initial sync: slot \"%s\" is usable on only %d of %d source nodes",
						s->cfg->seg_slot, have, s->nsrcsegs + 1),
				 errdetail("The rest are missing, or were invalidated by the publisher because the WAL they asked for is gone."),
				 errhint("Drop it everywhere with gp_drop_replication_slot() and retry.")));

	elog(LOG, "mpp apply worker: subscription %u: creating cluster-wide slot \"%s\" on the source",
		 s->cfg->subid, s->cfg->seg_slot);
	res = sync_exec(s, s->src, "source",
					"SELECT count(*) FROM gp_create_logical_replication_slot($1, 'pgoutput', false, true)",
					1, params);
	have = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	if (have != s->nsrcsegs + 1)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("initial sync: created slot \"%s\" on %d of %d source nodes",
						s->cfg->seg_slot, have, s->nsrcsegs + 1)));
}

/* one REPEATABLE READ transaction reading one column of the snapshot */
static int64
sync_snapshot_field(LrSync *s, const char *field)
{
	PGresult   *res;
	char	   *sql = psprintf("SELECT %s FROM gp_current_distributed_snapshot()", field);
	int64		v;

	PQclear(sync_exec(s, s->src, "source", "BEGIN ISOLATION LEVEL REPEATABLE READ", 0, NULL));
	res = sync_exec(s, s->src, "source", sql, 0, NULL);
	v = strtoll(PQgetvalue(res, 0, 0), NULL, 10);
	PQclear(res);
	PQclear(sync_exec(s, s->src, "source", "COMMIT", 0, NULL));
	pfree(sql);
	return v;
}

static void
sync_barrier(LrSync *s)
{
	int64		xmax_pre = sync_snapshot_field(s, "xmax");
	TimestampTz started = GetCurrentTimestamp();

	for (;;)
	{
		int64		xmin = sync_snapshot_field(s, "xmin");

		if (xmin >= xmax_pre)
		{
			elog(LOG, "mpp apply worker: subscription %u: barrier passed (xmin " INT64_FORMAT " >= " INT64_FORMAT ")",
				 s->cfg->subid, xmin, xmax_pre);
			return;
		}
		if (TimestampDifferenceExceeds(started, GetCurrentTimestamp(), LR_SYNC_BARRIER_TIMEOUT_MS))
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("initial sync: distributed transactions in flight at slot creation did not finish within %d s (xmin " INT64_FORMAT ", waiting for " INT64_FORMAT ")",
							LR_SYNC_BARRIER_TIMEOUT_MS / 1000, xmin, xmax_pre)));
		sync_wait(s, 200);
	}
}

/* opens the source transaction that every COPY runs in */
static void
sync_cut_point(LrSync *s)
{
	PGresult   *res;
	StringInfoData ds;
	char	   *ip;

	PQclear(sync_exec(s, s->src, "source", "BEGIN ISOLATION LEVEL REPEATABLE READ", 0, NULL));
	res = sync_exec(s, s->src, "source",
					"SELECT pg_current_wal_lsn()::text, xmax, coalesce(array_to_string(in_progress, ','), '') "
					"FROM gp_current_distributed_snapshot()",
					0, NULL);
	s->lt = pstrdup(PQgetvalue(res, 0, 0));
	s->ds_xmax = strtoll(PQgetvalue(res, 0, 1), NULL, 10);
	ip = pstrdup(PQgetvalue(res, 0, 2));
	PQclear(res);

	if (ip[0] != '\0')
	{
		char	   *save = NULL;
		char	   *tok;

		for (tok = strtok_r(ip, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
		{
			int64	   *g = palloc(sizeof(int64));

			*g = strtoll(tok, NULL, 10);
			s->ds_inprogress = lappend(s->ds_inprogress, g);
		}
	}

	initStringInfo(&ds);
	appendStringInfo(&ds, "xmax=" INT64_FORMAT ";inprogress=", s->ds_xmax);
	{
		ListCell   *lc;

		foreach(lc, s->ds_inprogress)
			appendStringInfo(&ds, "%s" INT64_FORMAT, lc == list_head(s->ds_inprogress) ? "" : ",",
							 *(int64 *) lfirst(lc));
	}
	s->ds_text = ds.data;

	elog(LOG, "mpp apply worker: subscription %u: cut point L_t %s, DS_t %s",
		 s->cfg->subid, s->lt, s->ds_text);
}

/* "xmax=<n>;inprogress=<a,b,..>" -> cfg */
static void
sync_apply_snapshot(LrWorkerConfig *cfg, const char *text)
{
	const char *p;
	char	   *copy;
	char	   *save = NULL;
	char	   *tok;
	int			n = 0;
	int64	   *arr;

	if ((p = strstr(text, "xmax=")) == NULL)
		return;
	cfg->sync_ds_xmax = strtoll(p + 5, NULL, 10);
	cfg->nsync_ds_inprogress = 0;
	cfg->sync_ds_inprogress = NULL;
	if ((p = strstr(text, "inprogress=")) == NULL || p[11] == '\0')
		return;
	copy = pstrdup(p + 11);
	for (tok = copy; *tok; tok++)
		if (*tok == ',')
			n++;
	n++;
	arr = palloc(sizeof(int64) * n);
	n = 0;
	for (tok = strtok_r(copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save))
		arr[n++] = strtoll(tok, NULL, 10);
	cfg->sync_ds_inprogress = arr;
	cfg->nsync_ds_inprogress = n;
}

/* ------------------------------------------------------------------------
 * S4: one table over the segment-to-segment transport
 * ------------------------------------------------------------------------
 */

/*
 * The name of one copy job.  It names the helper's port file and the frames
 * its senders open with, and the receiving socket has no other
 * authentication -- whoever reaches it writes into the table being copied --
 * so the name carries 128 random bits and is a secret, not a label.  The
 * readable prefix is there for the log and for finding a leftover file.
 */
static char *
sync_new_job(LrSync *s, LrSyncRel *r)
{
	uint8		rnd[16];
	char		hex[sizeof(rnd) * 2 + 1];

	if (!pg_strong_random(rnd, sizeof(rnd)))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate a random copy job name")));
	for (int i = 0; i < (int) sizeof(rnd); i++)
		snprintf(hex + i * 2, 3, "%02x", rnd[i]);
	return psprintf("lr%u_%u_%s", s->cfg->subid, r->relid, hex);
}

/*
 * The copy of one table in four steps, so that the incremental job can be
 * driven from the apply loop without blocking it (LrSyncAddStep).  The
 * blocking wrapper below is what the initial synchronisation uses.
 */
static void
copy_begin(LrSync *s, LrSyncRel *r)
{
	StringInfoData sql;

	s->cur_job = sync_new_job(s, r);
	s->cur_targets = NULL;
	s->cur_src_rows = -1;
	sync_set_rel_state(s, r->relid, 'd', s->cur_job);

	/*
	 * The receiving session may be orphaned if this worker dies hard while
	 * its INSERT waits in gp_lr_copy_recv(); client_connection_check_interval
	 * makes that backend notice the lost client and abort instead of holding
	 * the table's lock until the helper's accept timeout.
	 */
	if (s->tgt == NULL)
		s->tgt = sync_connect_one(s->cfg->target_conninfo, "target coordinator",
								  " options='-c client_connection_check_interval=1000'");

	initStringInfo(&sql);
	if (r->direct)
	{
		/*
		 * The two sides place this table's rows the same way, so segment i's
		 * share of the source is segment i's share of the target: each target
		 * segment takes its one sender's rows straight into the table, with
		 * no redistribution and no plan of ours in the way.
		 *
		 * gp_enable_segment_copy_checking is left alone on purpose.  It is on
		 * by default and makes every segment recompute where each row belongs
		 * and refuse the ones that are not its own, which is exactly the
		 * assumption above stated as a check: if it ever does not hold, this
		 * fails loudly instead of putting rows on the wrong segment.
		 */
		char	   *prog = psprintf("cbdb_lr_copyhelper recv --job %s --seg <SEGID> --clients 1"
									" --port-range %d-%d%s%s",
									s->cur_job, s->port_lo, s->port_hi,
									s->crc ? " --crc" : "",
									s->bindaddr[0] ? psprintf(" --bindaddr %s", s->bindaddr) : "");
		char	   *lit = PQescapeLiteral(s->tgt, prog, strlen(prog));

		appendStringInfo(&sql, "COPY %s (%s) FROM PROGRAM %s ON SEGMENT",
						 r->qualified, r->collist, lit);
		PQfreemem(lit);
		pfree(prog);
	}
	else
	{
		/* target: the receiving INSERT runs until every source segment is done */
		appendStringInfo(&sql,
						 "INSERT INTO %s (%s) SELECT * FROM pg_catalog.gp_lr_copy_recv(%u::regclass, '%s', %d) AS x(%s)",
						 r->qualified, r->collist, r->relid, s->cur_job, s->nsrcsegs, r->coldefs);
	}
	if (!PQsendQuery(s->tgt, sql.data))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("initial sync, target: %s", PQerrorMessage(s->tgt))));
	s->tgt_busy = true;
	pfree(sql.data);
	s->cur_started = GetCurrentTimestamp();
}

/* have the receiving segments published their ports yet? */
static bool
copy_ports_ready(LrSync *s, LrSyncRel *r)
{
	PGresult   *res;
	char		jobbuf[80];
	char		wantbuf[16];
	const char *params[2] = {jobbuf, wantbuf};
	int			want = Min(s->nsrcsegs, s->ntgtsegs);

	/* the INSERT failing early would otherwise make us wait in vain */
	if (!PQconsumeInput(s->tgt) || !PQisBusy(s->tgt))
	{
		PGresult   *r1 = PQgetResult(s->tgt);
		char	   *msg = pstrdup(r1 ? PQresultErrorMessage(r1) : PQerrorMessage(s->tgt));

		while (r1)
		{
			PQclear(r1);
			r1 = PQgetResult(s->tgt);
		}
		s->tgt_busy = false;
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, target receive of %s ended early: %s",
						r->qualified, msg)));
	}

	snprintf(jobbuf, sizeof(jobbuf), "%s", s->cur_job);
	snprintf(wantbuf, sizeof(wantbuf), "%d", want);
	res = sync_exec(s, s->tgt2, "target",
					"SELECT string_agg(a.address || ':' || p.port, ',' ORDER BY p.segid) "
					"FROM pg_catalog.gp_lr_copy_ports($1) p "
					"JOIN gp_segment_configuration a ON a.content = p.segid AND a.role = 'p' "
					"HAVING count(*) = $2::int4",
					2, params);
	if (PQntuples(res) == 1 && !PQgetisnull(res, 0, 0))
	{
		s->cur_targets = pstrdup(PQgetvalue(res, 0, 0));
		PQclear(res);
		return true;
	}
	PQclear(res);
	if (TimestampDifferenceExceeds(s->cur_started, GetCurrentTimestamp(),
								   LR_SYNC_PORTS_TIMEOUT_MS))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("initial sync: the target segments did not publish %d copy ports for %s within %d s",
						want, r->qualified, LR_SYNC_PORTS_TIMEOUT_MS / 1000)));
	return false;
}

/* source: every segment sends its share to target (seg mod N) */
static void
copy_send(LrSync *s, LrSyncRel *r)
{
	StringInfoData sql;
	char	   *prog = psprintf("cbdb_lr_copyhelper send --job %s --seg <SEGID> --targets %s%s",
								s->cur_job, s->cur_targets, s->crc ? " --crc" : "");
	char	   *lit = PQescapeLiteral(s->src, prog, strlen(prog));

	initStringInfo(&sql);
	appendStringInfo(&sql, "COPY (SELECT %s FROM %s) TO PROGRAM %s ON SEGMENT",
					 r->collist, r->qualified, lit);
	PQfreemem(lit);
	pfree(prog);
	if (!PQsendQuery(s->src, sql.data))
	{
		char	   *msg = pstrdup(PQerrorMessage(s->src));

		pfree(sql.data);
		sync_cancel_tgt(s);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, source COPY of %s: %s", r->qualified, msg)));
	}
	s->src_busy = true;
	pfree(sql.data);
}

/*
 * Both sides finished?  Collects the two results, compares the row counts
 * and marks the table copied.
 */
static bool
copy_poll_done(LrSync *s, LrSyncRel *r)
{
	PGresult   *res;
	int64		tgt_rows = -1;

	if (s->src_busy)
	{
		if (!PQconsumeInput(s->src))
		{
			char	   *msg = pstrdup(PQerrorMessage(s->src));

			s->src_busy = false;
			sync_cancel_tgt(s);
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("initial sync, source COPY of %s: %s", r->qualified, msg)));
		}
		if (PQisBusy(s->src))
			return false;

		{
			char	   *msg = NULL;

			while ((res = PQgetResult(s->src)) != NULL)
			{
				if (PQresultStatus(res) == PGRES_COMMAND_OK)
					s->cur_src_rows = strtoll(PQcmdTuples(res), NULL, 10);
				else if (msg == NULL)
					msg = pstrdup(PQresultErrorMessage(res));
				PQclear(res);
			}
			s->src_busy = false;
			if (msg)
			{
				/* stop the receiving side instead of letting it time out */
				sync_cancel_tgt(s);
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_EXCEPTION),
						 errmsg("initial sync, source COPY of %s: %s", r->qualified, msg)));
			}
		}
	}

	/* target: collect the INSERT */
	if (!PQconsumeInput(s->tgt))
	{
		s->tgt_busy = false;
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("initial sync, target receive of %s: %s",
						r->qualified, PQerrorMessage(s->tgt))));
	}
	if (PQisBusy(s->tgt))
		return false;
	{
		char	   *msg = NULL;

		while ((res = PQgetResult(s->tgt)) != NULL)
		{
			if (PQresultStatus(res) == PGRES_COMMAND_OK)
				tgt_rows = strtoll(PQcmdTuples(res), NULL, 10);
			else if (msg == NULL)
				msg = pstrdup(PQresultErrorMessage(res));
			PQclear(res);
		}
		s->tgt_busy = false;
		if (msg)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_EXCEPTION),
					 errmsg("initial sync, target receive of %s: %s", r->qualified, msg)));
	}

	if (s->cur_src_rows != tgt_rows)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("initial sync: %s: source sent " INT64_FORMAT " rows, target received " INT64_FORMAT,
						r->qualified, s->cur_src_rows, tgt_rows)));

	sync_set_rel_state(s, r->relid, 'f', NULL);
	elog(LOG, "mpp apply worker: subscription %u: copied %s: " INT64_FORMAT " rows, %s (job %s)",
		 s->cfg->subid, r->qualified, tgt_rows,
		 r->direct ? "loaded where it landed" : "redistributed", s->cur_job);
	return true;
}

static void
sync_copy_table(LrSync *s, LrSyncRel *r)
{
	if (r->replicated)
	{
		copy_via_coordinators(s, r);
		return;
	}
	copy_begin(s, r);
	while (!copy_ports_ready(s, r))
		sync_wait(s, LR_SYNC_POLL_MS);
	copy_send(s, r);
	while (!copy_poll_done(s, r))
		sync_wait(s, LR_SYNC_POLL_MS);
}


/* ------------------------------------------------------------------------
 * Incremental synchronisation of tables added to a running subscription:
 * started and stepped by the apply driver so that the stream of the other
 * tables keeps flowing while the new table is copied.
 *
 *   LrSyncPendingNames()  which tables are not ready (the driver defers them)
 *   LrSyncAddStart()      cut point of its own, first table's COPY started
 *   LrSyncAddStep()       one non-blocking step; true when every table is in
 *   LrSyncAddFinish()     after the driver caught the tables up: state 'r'
 * ------------------------------------------------------------------------
 */

/*
 * Names ("nsp.rel", unquoted, as the reader reports them) of the tables of
 * this subscription that may not receive changes yet.  Answered over the
 * driver's own session, which is idle between batches.
 */
List *
LrSyncPendingNames(PGconn *qd, Oid subid)
{
	char		subidbuf[16];
	const char *params[1] = {subidbuf};
	PGresult   *res;
	List	   *names = NIL;

	snprintf(subidbuf, sizeof(subidbuf), "%u", subid);
	res = PQexecParams(qd,
					   "SELECT n.nspname || '.' || c.relname"
					   "  FROM pg_catalog.gp_subscription_rel r"
					   "  JOIN pg_class c ON c.oid = r.relid"
					   "  JOIN pg_namespace n ON n.oid = c.relnamespace"
					   " WHERE r.subid = $1 AND r.relid <> 0 AND r.state NOT IN ('r', 's')"
					   " ORDER BY 1",
					   1, NULL, params, NULL, NULL, 0);
	if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		char	   *msg = pstrdup(res ? PQresultErrorMessage(res) : PQerrorMessage(qd));

		if (res)
			PQclear(res);
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_EXCEPTION),
				 errmsg("mpp apply worker: reading gp_subscription_rel: %s", msg)));
	}
	for (int i = 0; i < PQntuples(res); i++)
		names = lappend(names, pstrdup(PQgetvalue(res, i, 0)));
	PQclear(res);
	return names;
}

/*
 * Begin copying every pending table under one fresh cut point.  Returns NULL
 * when there is nothing to do.  The tables are truncated first: the driver
 * only started deferring their changes when it noticed them, so whatever the
 * target holds may already be in the COPY.
 *
 * No S2 barrier here (unlike the initial sync): the slots exist and the
 * readers have been buffering the whole stream all along, so no transaction
 * can fall between "not in the stream" and "not in the snapshot".
 */
/* the body of LrSyncAddStart, so that it can run under PG_TRY */
static void
add_start_body(LrSync *s)
{
	ListCell   *lc;

	sync_connect_source(s);

	/* redo from scratch: empty the tables, so the COPY lands in empty ones */
	PQclear(sync_exec(s, s->tgt2, "target", "BEGIN", 0, NULL));
	foreach(lc, s->rels)
	{
		LrSyncRel  *r = (LrSyncRel *) lfirst(lc);
		char	   *sql = psprintf("TRUNCATE TABLE %s", r->qualified);

		PQclear(sync_exec(s, s->tgt2, "target", sql, 0, NULL));
		pfree(sql);
	}
	PQclear(sync_exec(s, s->tgt2, "target", "COMMIT", 0, NULL));

	sync_cut_point(s);
	s->cur = list_head(s->rels);
	if (((LrSyncRel *) lfirst(s->cur))->replicated)
		s->step = LR_SYNC_STEP_INIT;	/* copied through the coordinators */
	else
	{
		copy_begin(s, (LrSyncRel *) lfirst(s->cur));
		s->step = LR_SYNC_STEP_PORTS;
	}

	elog(LOG, "mpp apply worker: subscription %u: synchronising %d added table(s), cut point L_t %s, DS_t %s",
		 s->cfg->subid, list_length(s->rels), s->lt, s->ds_text);
}

LrSync *
LrSyncAddStart(LrWorkerConfig *cfg)
{
	LrSync	   *s;
	MemoryContext cxt;
	MemoryContext oldcxt;

	cxt = AllocSetContextCreate(TopMemoryContext, "LrSync add", ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);
	s = palloc0(sizeof(LrSync));
	s->cfg = cfg;
	s->cxt = cxt;
	s->step = LR_SYNC_STEP_INIT;

	PG_TRY();
	{
		sync_connect(s);
		sync_fetch_pending(s);
		if (s->rels != NIL)
			add_start_body(s);
	}
	PG_CATCH();
	{
		sync_disconnect(s);
		MemoryContextSwitchTo(oldcxt);
		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcxt);
	if (s->rels == NIL)
	{
		sync_disconnect(s);
		MemoryContextDelete(cxt);
		return NULL;
	}
	return s;
}

/* one non-blocking step; true once every table has been copied */
bool
LrSyncAddStep(LrSync *s)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(s->cxt);
	LrSyncRel  *r;
	bool		done = false;

	if (s->step == LR_SYNC_STEP_DONE)
	{
		MemoryContextSwitchTo(oldcxt);
		return true;
	}
	r = (LrSyncRel *) lfirst(s->cur);

	/*
	 * A replicated table goes through the coordinators in one step; it is
	 * small by construction, so the apply loop does not stall noticeably.
	 */
	if (s->step == LR_SYNC_STEP_INIT)
	{
		copy_via_coordinators(s, r);
		s->cur = lnext(s->rels, s->cur);
		if (s->cur == NULL)
		{
			PQclear(sync_exec(s, s->src, "source", "COMMIT", 0, NULL));
			s->step = LR_SYNC_STEP_DONE;
			done = true;
		}
		else if (((LrSyncRel *) lfirst(s->cur))->replicated)
			s->step = LR_SYNC_STEP_INIT;
		else
		{
			copy_begin(s, (LrSyncRel *) lfirst(s->cur));
			s->step = LR_SYNC_STEP_PORTS;
		}
	}
	else if (s->step == LR_SYNC_STEP_PORTS)
	{
		if (copy_ports_ready(s, r))
		{
			copy_send(s, r);
			s->step = LR_SYNC_STEP_COPY;
		}
	}
	else if (s->step == LR_SYNC_STEP_COPY)
	{
		if (copy_poll_done(s, r))
		{
			s->cur = lnext(s->rels, s->cur);
			if (s->cur == NULL)
			{
				PQclear(sync_exec(s, s->src, "source", "COMMIT", 0, NULL));
				s->step = LR_SYNC_STEP_DONE;
				done = true;
			}
			else if (((LrSyncRel *) lfirst(s->cur))->replicated)
				s->step = LR_SYNC_STEP_INIT;	/* through the coordinators */
			else
			{
				copy_begin(s, (LrSyncRel *) lfirst(s->cur));
				s->step = LR_SYNC_STEP_PORTS;
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);
	return done;
}

/*
 * The cut point's WAL position on the source.  The driver may not publish
 * the tables until its own stream has passed it: a transaction that
 * committed before the cut point is inside the copy, and if it reaches the
 * apply loop after the table goes live nothing holds its rows back any more.
 */
int64
LrSyncAddCutLsn(LrSync *s)
{
	bool		err = false;

	if (s->lt == NULL)
		return 0;
	return (int64) pg_lsn_in_internal(s->lt, &err);
}

/* the cut point of this job: what the driver filters the catch-up with */
void
LrSyncAddSnapshot(LrSync *s, int64 *xmax, int *ninprogress, int64 **inprogress)
{
	ListCell   *lc;
	int			n = 0;

	*xmax = s->ds_xmax;
	*ninprogress = list_length(s->ds_inprogress);
	*inprogress = palloc(sizeof(int64) * Max(*ninprogress, 1));
	foreach(lc, s->ds_inprogress)
		(*inprogress)[n++] = *(int64 *) lfirst(lc);
}

/* the tables of this job, "nsp.rel" as the reader names them */
List *
LrSyncAddNames(LrSync *s)
{
	List	   *names = NIL;
	ListCell   *lc;

	foreach(lc, s->rels)
	{
		LrSyncRel  *r = (LrSyncRel *) lfirst(lc);
		char	   *copy = pstrdup(r->qualified);
		char	   *out = copy;
		char	   *in = copy;

		/* "nsp"."rel" -> nsp.rel */
		for (; *in; in++)
			if (*in != '"')
				*out++ = *in;
		*out = '\0';
		names = lappend(names, copy);
	}
	return names;
}

/* every table of this job -> 'r' with the cut point ('s' if untracked) */
static void
sync_finish_rels(LrSync *s)
{
	char		subid[16];
	char		relid[16];
	char		state[2];
	const char *params[5] = {subid, relid, s->lt, s->ds_text, state};
	ListCell   *lc;

	snprintf(subid, sizeof(subid), "%u", s->cfg->subid);
	PQclear(sync_exec(s, s->tgt2, "target", "BEGIN", 0, NULL));
	foreach(lc, s->rels)
	{
		LrSyncRel  *r = (LrSyncRel *) lfirst(lc);

		snprintf(relid, sizeof(relid), "%u", r->relid);
		state[0] = r->tracked ? 'r' : 's';
		state[1] = '\0';
		PQclear(sync_exec(s, s->tgt2, "target",
						  "UPDATE pg_catalog.gp_subscription_rel SET state = $5, sync_lsn = $3::pg_lsn, "
						  "sync_snapshot = $4, copy_job = NULL WHERE subid = $1 AND relid = $2",
						  5, params));
	}
	PQclear(sync_exec(s, s->tgt2, "target", "COMMIT", 0, NULL));
}

/* the driver caught the tables up: publish them and drop the job */
void
LrSyncAddFinish(LrSync *s)
{
	MemoryContext cxt = s->cxt;

	sync_finish_rels(s);
	elog(LOG, "mpp apply worker: subscription %u: %d added table(s) ready",
		 s->cfg->subid, list_length(s->rels));
	sync_disconnect(s);
	MemoryContextDelete(cxt);
}

/* give up: the tables stay pending and the next attempt redoes them */
void
LrSyncAddAbort(LrSync *s)
{
	MemoryContext cxt = s->cxt;

	PG_TRY();
	{
		sync_disconnect(s);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();
	MemoryContextDelete(cxt);
}

/* ------------------------------------------------------------------------
 * Catalog-side helpers for CREATE / DROP SUBSCRIPTION (subscriptioncmds.c)
 *
 * gp_subscription_* are ordinary coordinator-only tables created by
 * system_views.sql, so they are reached by name and written with the heap
 * API inside the caller's transaction.
 * ------------------------------------------------------------------------
 */

static Relation
sync_open_state_table(const char *name, LOCKMODE lockmode)
{
	RangeVar   *rv = makeRangeVar("pg_catalog", pstrdup(name), -1);

	return table_openrv(rv, lockmode);
}

/*
 * copy_data = false: the tables are already in sync; write the marker as
 * 'r' so the worker never copies.
 */
void
LrSyncMarkNoCopy(Oid subid)
{
	Relation	rel = sync_open_state_table("gp_subscription_rel", RowExclusiveLock);
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[6];
	bool		nulls[6];
	HeapTuple	tup;

	Assert(desc->natts == 6);
	memset(nulls, 0, sizeof(nulls));
	values[0] = ObjectIdGetDatum(subid);
	values[1] = ObjectIdGetDatum(LR_SYNC_MARKER_RELID);
	values[2] = CharGetDatum('r');
	nulls[3] = nulls[4] = nulls[5] = true;
	tup = heap_form_tuple(desc, values, nulls);
	CatalogTupleInsert(rel, tup);
	heap_freetuple(tup);
	table_close(rel, NoLock);
}

/*
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION: bring gp_subscription_rel in
 * line with the publication's table list.  Tables that are new get a row in
 * state 'i' ('r' with copy_data = false), tables that left the publication
 * lose theirs.  The running apply worker picks the new rows up within a
 * couple of seconds: it defers their changes, copies them and catches them
 * up.  Nothing here connects anywhere, so it is
 * transactional like the rest of the statement.
 */
void
LrSyncRefresh(Oid subid, List *relids, bool copy_data)
{
	Relation	rel = sync_open_state_table("gp_subscription_rel", RowExclusiveLock);
	TupleDesc	desc = RelationGetDescr(rel);
	ScanKeyData key;
	TableScanDesc scan;
	HeapTuple	tup;
	List	   *known = NIL;
	ListCell   *lc;
	int			nadded = 0,
				nremoved = 0;

	Assert(desc->natts == 6);

	ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(subid));
	scan = table_beginscan_catalog(rel, 1, &key);
	while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum		values[6];
		bool		nulls[6];
		Oid			relid;

		heap_deform_tuple(tup, desc, values, nulls);
		relid = DatumGetObjectId(values[1]);
		if (relid == LR_SYNC_MARKER_RELID)
			continue;
		if (list_member_oid(relids, relid))
			known = lappend_oid(known, relid);
		else
		{
			CatalogTupleDelete(rel, &tup->t_self);
			nremoved++;
		}
	}
	table_endscan(scan);

	foreach(lc, relids)
	{
		Oid			relid = lfirst_oid(lc);
		Datum		values[6];
		bool		nulls[6];

		if (list_member_oid(known, relid))
			continue;
		memset(nulls, 0, sizeof(nulls));
		values[0] = ObjectIdGetDatum(subid);
		values[1] = ObjectIdGetDatum(relid);
		values[2] = CharGetDatum(copy_data ? 'i' : 'r');
		nulls[3] = nulls[4] = nulls[5] = true;
		tup = heap_form_tuple(desc, values, nulls);
		CatalogTupleInsert(rel, tup);
		heap_freetuple(tup);
		nadded++;
	}
	table_close(rel, NoLock);

	ereport(NOTICE,
			(errmsg("%d table(s) added to the MPP subscription, %d removed",
					nadded, nremoved)));
}

/* DROP SUBSCRIPTION: forget the subscription's state rows */
void
LrSyncCleanup(Oid subid)
{
	static const char *const tables[] = {
		"gp_subscription_rel", "gp_subscription_progress", "gp_subscription_applied"
	};

	for (int i = 0; i < lengthof(tables); i++)
	{
		Relation	rel = sync_open_state_table(tables[i], RowExclusiveLock);
		ScanKeyData key;
		TableScanDesc scan;
		HeapTuple	tup;

		/* subid is the first column of all three tables */
		ScanKeyInit(&key, 1, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(subid));
		scan = table_beginscan_catalog(rel, 1, &key);
		while ((tup = heap_getnext(scan, ForwardScanDirection)) != NULL)
			CatalogTupleDelete(rel, &tup->t_self);
		table_endscan(scan);
		table_close(rel, NoLock);
	}
}
