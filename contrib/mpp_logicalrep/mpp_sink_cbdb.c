/*-------------------------------------------------------------------------
 *
 * mpp_sink_cbdb.c
 *	  The Cloudberry sink: a unit of assembled transactions becomes one
 *	  distributed transaction of the target.
 *
 * This is the LogicalRepSink implementation #1 (design section 4.5).  It is
 * the out-of-band kind: the rows it applies never pass through this
 * process.  The reader gang holds them, the plan gp_lr_apply dispatches
 * moves them to the writer gang over the interconnect, and all the sink
 * sends is the list of transactions each segment should replay.
 *
 * One unit is:
 *
 *	  BEGIN
 *	  LOCK TABLE ... 				  every relation of the unit, in OID order
 *	  SELECT gp_lr_apply(...)		  the readers replay, the writers apply
 *	  SELECT gp_lr_progress(...)	  the position, in the same transaction
 *	  COMMIT						  one distributed (two-phase) commit
 *
 * The locks come first and in OID order because the coordinator takes them
 * before the writers do, which is what keeps two apply units and ordinary
 * user statements from deadlocking across segments (design section 8.3).
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_sink_cbdb.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "lib/stringinfo.h"
#include "mpp_apply.h"
#include "mpp_apply_internal.h"
#include "storage/lockdefs.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/pg_lsn.h"

typedef struct LrCbdbSink
{
	LrCbdbTransport *tr;
	MemoryContext cxt;			/* one unit */
	bool		in_xact;		/* a target transaction is open */
} LrCbdbSink;

/* one transaction of one segment, as gp_lr_apply wants it */
typedef struct LrSinkItem
{
	int			segidx;
	int64		pos;
	uint64		gxid;			/* 0 = addressed by position alone */
} LrSinkItem;

static int
cmp_sink_item(const void *a, const void *b)
{
	const LrSinkItem *x = a,
			   *y = b;

	if (x->segidx != y->segidx)
		return x->segidx < y->segidx ? -1 : 1;
	return x->pos < y->pos ? -1 : x->pos > y->pos;
}

typedef struct LrSinkLock
{
	int64		oid;
	char	   *quoted;
	int			lockmode;
} LrSinkLock;

static int
cmp_sink_lock(const void *a, const void *b)
{
	const LrSinkLock *x = a,
			   *y = b;

	return x->oid < y->oid ? -1 : x->oid > y->oid;
}

static const char *
lockmode_name(int lockmode)
{
	return lockmode >= AccessExclusiveLock ? "ACCESS EXCLUSIVE" : "ROW EXCLUSIVE";
}

/* ------------------------------------------------------------------------
 * Life cycle
 * ------------------------------------------------------------------------
 */

static void
cbdb_sink_startup(LogicalRepSink *self, void *cfg)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;

	if (s->tr->qd == NULL)
		LrTransportConnect(s->tr);
}

static void
cbdb_sink_shutdown(LogicalRepSink *self)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;

	LrTransportClose(s->tr);
	s->in_xact = false;
}

static SinkCaps
cbdb_sink_capabilities(LogicalRepSink *self)
{
	SinkCaps	caps;

	memset(&caps, 0, sizeof(caps));

	/*
	 * Rows travel from the reader gang to the writer gang inside the target
	 * cluster; the driver never sees one.
	 */
	caps.delivers_rows = false;

	/* many transactions, one distributed commit */
	caps.commits_units = true;

	caps.needs_participant_set = true;	/* which segments replay what */
	caps.needs_relid_enumeration_for_lock = true;	/* coordinator locks first */
	caps.needs_redistribution = true;	/* the target's own distribution */
	caps.does_2pc = true;
	caps.needs_type_mapping = false;	/* same engine on both ends */

	return caps;
}

/*
 * The progress the target has committed.  Reconnects first if an error took
 * the session away, so that this is also the first step of a recovery.
 */
static ProgressPos
cbdb_sink_get_checkpoint(LogicalRepSink *self)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;
	ProgressPos pos;
	PGresult   *res;
	char		subid[16];
	const char *params[1] = {subid};

	if (s->tr->qd == NULL)
		LrTransportConnect(s->tr);
	s->in_xact = false;

	/*
	 * The arrays are as long as the source has segments, which is neither the
	 * target's segment count nor known here: read back exactly what was
	 * written and let the assembly size itself from its own source list.
	 */
	memset(&pos, 0, sizeof(pos));

	snprintf(subid, sizeof(subid), "%u", s->tr->subid);
	res = LrTransportExecParams(s->tr,
								"SELECT coordinator_lsn::text, array_to_string(seg_pos, ','), array_to_string(seg_flush, ',') "
								"FROM pg_catalog.gp_subscription_progress WHERE subid = $1",
								1, params);
	if (PQntuples(res) == 1)
	{
		bool		err = false;

		const char *list = PQgetvalue(res, 0, 1);

		if (!PQgetisnull(res, 0, 0))
			pos.commit_order_pos = pg_lsn_in_internal(PQgetvalue(res, 0, 0), &err);

		if (*list != '\0')
		{
			pos.nsegs = 1;
			for (const char *c = list; *c; c++)
				if (*c == ',')
					pos.nsegs++;
		}
		if (pos.nsegs > 0)
		{
			pos.seg_pos = palloc0(sizeof(XLogRecPtr) * pos.nsegs);
			pos.seg_flush = palloc0(sizeof(XLogRecPtr) * pos.nsegs);
			for (int col = 1; col <= 2; col++)
			{
				char	   *copy = pstrdup(PQgetvalue(res, 0, col));
				char	   *save = NULL;
				char	   *tok;
				int			i = 0;

				for (tok = strtok_r(copy, ",", &save); tok && i < pos.nsegs;
					 tok = strtok_r(NULL, ",", &save), i++)
					(col == 1 ? pos.seg_pos : pos.seg_flush)[i] =
						(XLogRecPtr) strtoll(tok, NULL, 10);
				pfree(copy);
			}
		}
	}
	PQclear(res);

	return pos;
}

/* ------------------------------------------------------------------------
 * One unit
 * ------------------------------------------------------------------------
 */

/*
 * Open the transaction and take every lock the unit needs, in OID order.
 *
 * A relation the target does not have is an error: the subscription names a
 * table that is not there, and applying the rest would silently lose rows.
 */
static void
cbdb_sink_begin_unit(LogicalRepSink *self, const LogicalRepUnit *unit)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;
	MemoryContext oldcxt = MemoryContextSwitchTo(s->cxt);
	LrSinkLock *locks;
	int			nlocks = 0;

	locks = palloc(sizeof(LrSinkLock) * Max(unit->nrels, 1));
	for (int i = 0; i < unit->nrels; i++)
	{
		const LogicalRepRelLock *rl = &unit->rels[i];
		const char *params[1];
		PGresult   *res;
		char	   *quoted;

		quoted = LrTransportQuoteRel(s->tr, rl->rel.nspname, rl->rel.relname);
		params[0] = quoted;
		res = LrTransportExecParams(s->tr, "SELECT to_regclass($1)::oid", 1, params);
		if (PQgetisnull(res, 0, 0))
		{
			PQclear(res);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("logical replication target relation %s does not exist",
							quoted)));
		}
		locks[nlocks].oid = strtoll(PQgetvalue(res, 0, 0), NULL, 10);
		locks[nlocks].quoted = quoted;
		locks[nlocks].lockmode = rl->lockmode;
		nlocks++;
		PQclear(res);
	}
	qsort(locks, nlocks, sizeof(LrSinkLock), cmp_sink_lock);

	LrTransportExec(s->tr, "BEGIN");
	s->in_xact = true;

	for (int i = 0; i < nlocks; i++)
	{
		StringInfoData sql;

		initStringInfo(&sql);
		appendStringInfo(&sql, "LOCK TABLE %s IN %s MODE",
						 locks[i].quoted, lockmode_name(locks[i].lockmode));
		PQclear(LrTransportExec(s->tr, sql.data));
		pfree(sql.data);
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * The out-of-band apply: name the transactions, let the gangs move the rows.
 *
 * Within one segment the transactions are replayed in the order of that
 * segment's stream, not in commit order -- two one-phase transactions of the
 * same segment can reach the coordinator in the other order, and the stream
 * is the one that tells the truth (design section 8.4).
 */
static void
cbdb_sink_apply_unit(LogicalRepSink *self, const LogicalRepUnit *unit)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;
	const LrCbdbUnitSpec *spec = (const LrCbdbUnitSpec *) unit->transport_private;
	MemoryContext oldcxt = MemoryContextSwitchTo(s->cxt);
	LrSinkItem *items;
	int			nitems = 0;
	StringInfoData txns;
	PGresult   *res;
	char		subid[16],
				token[32];
	int			napplied = 0,
				ncold = 0;

	/* flatten (segment, position, gxid) over the unit, then order it */
	nitems = 0;
	for (int i = 0; i < unit->ntxns; i++)
		nitems += unit->txns[i].nparticipants;
	items = palloc(sizeof(LrSinkItem) * Max(nitems, 1));
	nitems = 0;
	for (int i = 0; i < unit->ntxns; i++)
	{
		const AssembledTxn *t = &unit->txns[i];

		for (int p = 0; p < t->nparticipants; p++)
		{
			items[nitems].segidx = t->participant_segidx[p];
			items[nitems].pos = (int64) t->participant_pos[p];
			items[nitems].gxid = t->gxid;
			nitems++;
		}
	}
	qsort(items, nitems, sizeof(LrSinkItem), cmp_sink_item);

	initStringInfo(&txns);
	appendStringInfoChar(&txns, '{');
	for (int i = 0; i < nitems; i++)
	{
		if (txns.len > 1)
			appendStringInfoChar(&txns, ',');
		if (items[i].gxid != 0)
			appendStringInfo(&txns, "\"%d:" UINT64_FORMAT "\"", items[i].segidx,
							 items[i].gxid);
		else
			appendStringInfo(&txns, "\"%d:L" INT64_FORMAT "\"", items[i].segidx,
							 items[i].pos);
	}
	appendStringInfoChar(&txns, '}');

	snprintf(subid, sizeof(subid), "%u", s->tr->subid);
	snprintf(token, sizeof(token), INT64_FORMAT, s->tr->token);

	{
		const char *params[5] = {subid, token, txns.data,
			spec ? spec->skip_relids : NULL,
			spec ? spec->only_relids : NULL};

		res = LrTransportExecParams(s->tr,
									"SELECT seg, kind, nrows FROM pg_catalog.gp_lr_apply($1, $2, $3::text[], $4::oid[], $5::oid[])",
									5, params);
		for (int i = 0; i < PQntuples(res); i++)
		{
			char		kind = PQgetvalue(res, i, 1)[0];

			if (kind == LR_REPORT_COLD)
				ncold++;
			else if (kind == LR_REPORT_APPLIED)
			{
				napplied++;
				s->tr->stats->rows_applied += strtoll(PQgetvalue(res, i, 2), NULL, 10);
			}
		}
		PQclear(res);
	}

	/*
	 * A cold reader lost its state (its session was reset): nothing of this
	 * unit is trustworthy, so give up on it and let the driver rebuild from
	 * the last committed position.
	 */
	if (ncold > 0 || napplied != s->tr->nsegs)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("gp_lr_apply%s: %d writers applied, %d readers cold",
						(spec && spec->catchup) ? " (catch-up)" : "",
						napplied, ncold)));

	MemoryContextSwitchTo(oldcxt);
}

/*
 * Record where the subscription now stands and commit.  The progress is
 * written in the same transaction as the rows on purpose: either both are
 * there after a crash, or neither is, and a replay never has to guess.
 */
static CommitResult
cbdb_sink_commit_unit(LogicalRepSink *self, const LogicalRepUnit *unit,
					  const ProgressPos *pos)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;
	MemoryContext oldcxt;
	StringInfoData spos,
				sflush,
				aseg,
				agx,
				alsn;
	const char *params[7];
	char		subid[16];
	char		clsn[32];

	/*
	 * A unit that moves no position records nothing: a catch-up replay of
	 * transactions that were already counted when they were first applied
	 * (design section 10.5).  It still commits, alone.
	 */
	if (pos->nsegs == 0)
	{
		PQclear(LrTransportExec(s->tr, "COMMIT"));
		s->in_xact = false;
		MemoryContextReset(s->cxt);
		return LR_COMMIT_OK;
	}

	oldcxt = MemoryContextSwitchTo(s->cxt);
	snprintf(subid, sizeof(subid), "%u", s->tr->subid);

	initStringInfo(&spos);
	initStringInfo(&sflush);
	appendStringInfoChar(&spos, '{');
	appendStringInfoChar(&sflush, '{');
	for (int seg = 0; seg < pos->nsegs; seg++)
	{
		appendStringInfo(&spos, "%s" INT64_FORMAT, seg ? "," : "",
						 (int64) pos->seg_pos[seg]);
		appendStringInfo(&sflush, "%s" INT64_FORMAT, seg ? "," : "",
						 (int64) pos->seg_flush[seg]);
	}
	appendStringInfoChar(&spos, '}');
	appendStringInfoChar(&sflush, '}');

	/* the deduplication triples (design 13.1) */
	initStringInfo(&aseg);
	initStringInfo(&agx);
	initStringInfo(&alsn);
	appendStringInfoChar(&aseg, '{');
	appendStringInfoChar(&agx, '{');
	appendStringInfoChar(&alsn, '{');
	for (int i = 0; i < pos->napplied; i++)
	{
		appendStringInfo(&aseg, "%s%d", aseg.len > 1 ? "," : "", pos->applied_segidx[i]);
		appendStringInfo(&agx, "%s" UINT64_FORMAT, agx.len > 1 ? "," : "", pos->applied_gxid[i]);
		appendStringInfo(&alsn, "%s" INT64_FORMAT, alsn.len > 1 ? "," : "",
						 (int64) pos->applied_pos[i]);
	}
	appendStringInfoChar(&aseg, '}');
	appendStringInfoChar(&agx, '}');
	appendStringInfoChar(&alsn, '}');

	snprintf(clsn, sizeof(clsn), "%X/%X", LSN_FORMAT_ARGS(pos->commit_order_pos));
	params[0] = subid;
	params[1] = clsn;
	params[2] = spos.data;
	params[3] = sflush.data;
	params[4] = aseg.data;
	params[5] = agx.data;
	params[6] = alsn.data;
	PQclear(LrTransportExecParams(s->tr,
								  "SELECT pg_catalog.gp_lr_progress($1, $2::pg_lsn, $3::int8[], $4::int8[], $5::int4[], $6::int8[], $7::int8[])",
								  7, params));

	PQclear(LrTransportExec(s->tr, "COMMIT"));
	s->in_xact = false;

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(s->cxt);

	/*
	 * Nothing is retried here.  The gangs are part of this session, so a
	 * failure that leaves them usable is rare enough not to be worth a
	 * special path: any error above became an ERROR, and the driver rebuilds
	 * from the position this function last committed.
	 */
	return LR_COMMIT_OK;
}

/* End whatever is open.  Called on every error path, so it may not throw. */
static void
cbdb_sink_abort(LogicalRepSink *self)
{
	LrCbdbSink *s = (LrCbdbSink *) self->private_data;

	s->in_xact = false;
	if (s->tr->qd != NULL && PQstatus(s->tr->qd) == CONNECTION_OK)
	{
		PGresult   *res = PQexec(s->tr->qd, "ROLLBACK");

		PQclear(res);
	}
	else
		LrTransportClose(s->tr);	/* get_checkpoint reopens it */

	MemoryContextReset(s->cxt);
}

/* ------------------------------------------------------------------------ */

LogicalRepSink *
LrMakeCbdbSink(LrCbdbTransport *tr)
{
	LogicalRepSink *sink = palloc0(sizeof(LogicalRepSink));
	LrCbdbSink *s = palloc0(sizeof(LrCbdbSink));

	s->tr = tr;
	s->cxt = AllocSetContextCreate(CurrentMemoryContext, "LrCbdbSink unit",
								   ALLOCSET_DEFAULT_SIZES);

	sink->startup = cbdb_sink_startup;
	sink->shutdown = cbdb_sink_shutdown;
	sink->capabilities = cbdb_sink_capabilities;
	sink->get_checkpoint = cbdb_sink_get_checkpoint;
	sink->begin_unit = cbdb_sink_begin_unit;
	sink->begin_txn = NULL;		/* not delivers_rows */
	sink->change = NULL;
	sink->apply_unit = cbdb_sink_apply_unit;
	sink->commit_unit = cbdb_sink_commit_unit;
	sink->abort = cbdb_sink_abort;
	sink->private_data = s;

	return sink;
}
