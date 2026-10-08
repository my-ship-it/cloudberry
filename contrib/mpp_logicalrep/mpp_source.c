/*-------------------------------------------------------------------------
 *
 * mpp_source.c
 *	  LogicalSource: the reader-gang operator of MPP logical replication apply.
 *
 * One LogicalSource runs on every reader QE.  Depending on the plan mode
 * (mpp_apply.h) it
 *
 *   CONNECT  builds this QE's reader state (mpp_reader.c) and connects it
 *            to its source segment; reports one 'C' row;
 *   DRAIN    releases the transactions the coordinator applied, reads the
 *            stream until the requested gxid is ready (participants) or
 *            until nothing more is buffered (others), sends feedback, and
 *            reports every newly ready transaction ('P' / 'L') plus one 'S'
 *            status row;
 *   APPLY    emits the changes of the listed transactions on the Explicit
 *            Motion channel, routed by the target table's distribution
 *            (mpp_route.c); the writer applies them.
 *
 * A reader with no state for the subscription, a different session token or
 * a lost connection reports 'X' (cold) and does nothing else; the
 * coordinator then rolls back and reconnects.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_source.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "catalog/pg_subscription.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "nodes/extensible.h"
#include "nodes/value.h"
#include "libpq/pqformat.h"
#include "mpp_apply.h"
#include "mpp_reader.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"

#define LR_DRAIN_DEFAULT_WAIT_MS	10000

/* a row waiting to be emitted */
typedef struct LrOutRow
{
	int32		target_segid;
	Oid			relid;
	char		op;
	char	   *data;			/* owned by the plan context */
	int			len;
} LrOutRow;

/* APPLY: the transactions this reader emits, in order */
typedef struct LrApplyItem
{
	struct LrReaderState *reader;	/* the stream that holds it */
	int64		gxid;			/* 0 for a local transaction */
	XLogRecPtr	local_lsn;
	int			nskip;			/* relations this transaction may not write */
	Oid		   *skip;
} LrApplyItem;

typedef struct LrKV
{
	char	   *key;
	char	   *value;
} LrKV;

/*
 * How many rows the apply queues between reads of the source.  Small enough
 * that the source never waits long, large enough that the read is not a
 * syscall per row: at roughly a megabyte of changes it is one read per
 * sixteen thousand of them.
 */
#define LR_APPLY_SIP_ROWS	16384

typedef struct LrSourceState
{
	CustomScanState css;
	LrPlanSpec	spec;
	bool		active;			/* this process runs the reader slice */
	bool		started;		/* mode work done */
	bool		done;
	bool		inject;			/* debug rows only */
	int64		nseq;			/* rows emitted so far */

	List	   *readers;		/* LrReaderState*, one per source segment this
								 * reader QE serves; NIL when cold */
	MemoryContext plancxt;		/* lives for one execution of the plan */
	List	   *kv;				/* parsed payload */

	/* output queue (reports and routed rows) */
	List	   *queue;			/* LrOutRow* */
	int			queue_pos;		/* next row to emit; a cell pointer would go
								 * stale when lappend() grows the list */

	/*
	 * APPLY filter.  filter_relids are remote relids of
	 * tables that are not ready to receive changes: normally they are skipped
	 * (their transactions stay buffered until the driver catches them up),
	 * and in a catch-up plan (catchup=) they are the only ones emitted.
	 */
	Oid		   *filter_relids;
	int			nfilter;
	bool		filter_only;

	/* APPLY iteration */
	List	   *items;			/* LrApplyItem* */
	ListCell   *item_next;
	LrReaderState *cur_reader;	/* the stream the open transaction is from */
	LrTxn	   *cur_txn;
	struct LrApplyItem *cur_item;	/* what the coordinator asked for it */
	LrChangeCursor cur;
	bool		cur_open;
	int			since_sip;		/* rows queued since the source was last read */
	LrRouter   *router;
} LrSourceState;

static Node *lr_source_create_state(CustomScan *cscan);
static void lr_source_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *lr_source_exec(CustomScanState *node);
static void lr_source_end(CustomScanState *node);
static void lr_source_rescan(CustomScanState *node);
static void lr_source_explain(CustomScanState *node, List *ancestors,
							  ExplainState *es);

static void source_start(LrSourceState *state);
static void parse_inject(LrSourceState *state, const char *payload);
static void do_connect(LrSourceState *state);
static void do_drain(LrSourceState *state);
static void apply_begin(LrSourceState *state);
static bool apply_fill_queue(LrSourceState *state);
static void apply_finish(LrSourceState *state);
static void queue_report(LrSourceState *state, const LrReport *rpt);
static void queue_row(LrSourceState *state, int32 target, Oid relid, char op,
					  const char *data, int len);
static void queue_cold(LrSourceState *state);
static bool reader_is_warm(LrSourceState *state);
static TupleTableSlot *source_store_row(LrSourceState *state, int32 target_segid,
										Oid relid, char op, const char *payload,
										int len);
static List *parse_kv(const char *payload);
static const char *kv_get(List *kv, const char *key);
static const char *kv_get_seg(List *kv, const char *key, int seg);
static List *parse_int64_list(const char *value);
static void parse_local_snapshot(const char *value, LrReaderStartPoint *start);
static void parse_ds_cut(const char *value, LrReaderStartPoint *start);
static XLogRecPtr parse_lsn(const char *value);

const CustomScanMethods LrSourceScanMethods = {
	"LogicalSource",
	lr_source_create_state
};

static const CustomExecMethods lr_source_exec_methods = {
	.CustomName = "LogicalSource",
	.BeginCustomScan = lr_source_begin,
	.ExecCustomScan = lr_source_exec,
	.EndCustomScan = lr_source_end,
	.ReScanCustomScan = lr_source_rescan,
	.ExplainCustomScan = lr_source_explain,
};

/* ------------------------------------------------------------------------
 * Operator
 * ------------------------------------------------------------------------
 */

static Node *
lr_source_create_state(CustomScan *cscan)
{
	LrSourceState *state = (LrSourceState *) newNode(sizeof(LrSourceState),
													 T_CustomScanState);

	state->css.methods = &lr_source_exec_methods;
	LrSpecFromCustomScan(cscan, &state->spec);
	return (Node *) state;
}

static void
lr_source_begin(CustomScanState *node, EState *estate, int eflags)
{
	LrSourceState *state = (LrSourceState *) node;

	state->active = LrSliceIsLocal(estate);
	state->started = false;
	state->done = false;
	state->inject = false;
	state->nseq = 0;
	state->readers = NIL;
	state->plancxt = NULL;
	state->kv = NIL;
	state->queue = NIL;
	state->queue_pos = 0;
	state->items = NIL;
	state->item_next = NULL;
	state->cur_txn = NULL;
	state->cur_item = NULL;
	state->cur_open = false;
	state->router = NULL;
	state->filter_relids = NULL;
	state->nfilter = 0;
	state->filter_only = false;

	if (!state->active || (eflags & EXEC_FLAG_EXPLAIN_ONLY))
		return;

	state->plancxt = AllocSetContextCreate(estate->es_query_cxt,
										   "LogicalSource plan",
										   ALLOCSET_DEFAULT_SIZES);

	elog(DEBUG1, "LogicalSource %s: subscription %u, dtx context %s",
		 LrPlanModeName(state->spec.mode), state->spec.subid,
		 DtxContextToString(DistributedTransactionContext));
}

/*
 * The mode's work happens on the first call, not in Begin: Begin runs on
 * every QE of every slice, and only the reader slice may touch the stream.
 */
static TupleTableSlot *
lr_source_next(ScanState *ss)
{
	LrSourceState *state = (LrSourceState *) ss;

	if (!state->active || state->done)
		return NULL;

	if (!state->started)
	{
		state->started = true;
		source_start(state);
	}

	for (;;)
	{
		if (state->queue_pos < list_length(state->queue))
		{
			LrOutRow   *row = (LrOutRow *) list_nth(state->queue, state->queue_pos);

			state->queue_pos++;
			return source_store_row(state, row->target_segid, row->relid,
									row->op, row->data, row->len);
		}

		/*
		 * A reader reads its source only while it is executing something, and
		 * replaying what it has already buffered is the longest something it
		 * ever does -- ten million rows took nineteen seconds of it, with the
		 * socket untouched throughout and the source blocked on a window
		 * nobody opened.  So take what has arrived, between rows, without
		 * waiting for any: this is not the drain, it only keeps the source
		 * moving while the drain is not running.
		 */
		if (state->spec.mode == LR_PLAN_APPLY &&
			state->since_sip >= LR_APPLY_SIP_ROWS)
		{
			ListCell   *lc;

			state->since_sip = 0;
			foreach(lc, state->readers)
			{
				LrReaderState *st = (LrReaderState *) lfirst(lc);

				if (LrReaderHasSource(st))
					(void) LrReaderReceive(st, 0, 0, false);
			}
		}

		/* queue drained; APPLY refills it record by record */
		if (state->queue != NIL)
		{
			ListCell   *lc;

			foreach(lc, state->queue)
				pfree(((LrOutRow *) lfirst(lc))->data);
			list_free_deep(state->queue);
			state->queue = NIL;
			state->queue_pos = 0;
		}

		if (state->spec.mode == LR_PLAN_APPLY && state->readers != NIL &&
			!state->inject)
		{
			if (apply_fill_queue(state))
				continue;
			apply_finish(state);
		}

		state->done = true;
		return NULL;
	}
}

static TupleTableSlot *
lr_source_exec(CustomScanState *node)
{
	return ExecScan(&node->ss, lr_source_next, LrScanRecheck);
}

static void
lr_source_end(CustomScanState *node)
{
	LrSourceState *state = (LrSourceState *) node;

	/* an early shutdown: close what APPLY opened */
	if (state->router)
		apply_finish(state);
}

static void
lr_source_rescan(CustomScanState *node)
{
	LrSourceState *state = (LrSourceState *) node;

	/* a rescan would redo the stream work; these plans never rescan */
	if (state->started)
		elog(ERROR, "LogicalSource cannot be rescanned");
}

static void
lr_source_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	LrSourceState *state = (LrSourceState *) node;

	LrExplainSpec(&state->spec, es);
}

/* ------------------------------------------------------------------------
 * Modes
 * ------------------------------------------------------------------------
 */

static void
source_start(LrSourceState *state)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(state->plancxt);
	const char *payload = state->spec.payload ? state->spec.payload : "";

	if (strncmp(payload, LR_INJECT_PREFIX, strlen(LR_INJECT_PREFIX)) == 0)
	{
		state->inject = true;
		parse_inject(state, payload + strlen(LR_INJECT_PREFIX));
	}
	else
	{
		state->kv = parse_kv(payload);
		switch (state->spec.mode)
		{
			case LR_PLAN_CONNECT:
				do_connect(state);
				break;
			case LR_PLAN_DRAIN:
				do_drain(state);
				break;
			case LR_PLAN_APPLY:
				apply_begin(state);
				break;
		}
	}
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Debug hook (mpp_apply.h): "[src@]target,relid,op,hex;..." rows; the reader
 * on src (default 0) queues its rows and emits nothing else.
 */
static void
parse_inject(LrSourceState *state, const char *rows)
{
	char	   *copy = pstrdup(rows);
	char	   *saveptr = NULL;
	char	   *tok;

	for (tok = strtok_r(copy, ";", &saveptr); tok != NULL;
		 tok = strtok_r(NULL, ";", &saveptr))
	{
		char	   *f[4];
		char	   *fsave = NULL;
		int			n = 0;
		int32		src = 0;
		int32		dst;
		Oid			relid;
		const char *hex;
		int			hexlen;
		char	   *bin;
		int			blen;

		for (char *p = strtok_r(tok, ",", &fsave); p != NULL && n < 4;
			 p = strtok_r(NULL, ",", &fsave))
			f[n++] = p;
		if (n < 3)
			elog(ERROR, "malformed inject row \"%s\"", tok);
		if (strchr(f[0], '@'))
		{
			src = (int32) strtol(f[0], NULL, 10);
			dst = (int32) strtol(strchr(f[0], '@') + 1, NULL, 10);
		}
		else
			dst = (int32) strtol(f[0], NULL, 10);
		if (src != GpIdentity.segindex)
			continue;
		relid = (Oid) strtoul(f[1], NULL, 10);
		hex = n == 4 ? f[3] : "";
		hexlen = strlen(hex);
		bin = palloc(hexlen / 2 + 1);
		blen = (int) hex_decode(hex, hexlen, bin);
		queue_row(state, dst, relid, f[2][0], bin, blen);
	}
}

/*
 * Every stream of this subscription exists, belongs to this session and is
 * still connected.
 *
 * A reader that serves no source segment (the source has fewer segments than
 * the target) has one state with no stream at all, but it is still a live
 * reader: it reports its state and takes part in the plans.
 */
static bool
reader_is_warm(LrSourceState *state)
{
	List	   *all = LrReaderListSub(state->spec.subid);
	ListCell   *lc;

	if (all == NIL)
		return false;
	foreach(lc, all)
	{
		LrReaderState *st = (LrReaderState *) lfirst(lc);

		if (LrReaderToken(st) != state->spec.token ||
			(!LrReaderIsConnected(st) && LrReaderHasSource(st)))
		{
			list_free(all);
			return false;
		}
	}
	state->readers = all;
	return true;
}

static void
queue_cold(LrSourceState *state)
{
	LrReport	rpt;

	memset(&rpt, 0, sizeof(rpt));
	rpt.seg = GpIdentity.segindex;
	rpt.kind = LR_REPORT_COLD;
	queue_report(state, &rpt);
}

/*
 * CONNECT: open one stream per source segment this reader was given.
 *
 * The driver spreads the source segments over the readers with
 * "localsnap.<srcseg>=xmin:xmax:xip,..." -- that segment's snapshot at the
 * initial copy's cut point, for the reader's own dedup of local transactions.
 *
 * "srcsegs.<reader>=a,b,..." -- one each when the counts match, several when
 * the source has more segments, none at all when it has fewer.  The
 * per-stream keys (conninfo, flush, pos, applied) are
 * indexed by SOURCE segment, not by reader.
 */
/*
 * The keywords of this subscription's conninfo that the CONNECT payload
 * deliberately does not carry -- its secrets -- as a conninfo fragment, or
 * NULL when it has none.  Read from this segment's own pg_subscription row,
 * which CREATE SUBSCRIPTION put here and which only a superuser can read.
 */
static const char *
source_secrets(Oid subid)
{
	Subscription *sub = GetSubscription(subid, true);
	PQconninfoOption *opts;
	char	   *err = NULL;
	StringInfoData buf;

	if (sub == NULL || sub->conninfo == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("subscription %u is not on this segment", subid),
				 errdetail("Its connection secrets are taken from the local catalog rather than from the dispatched payload.")));

	opts = PQconninfoParse(sub->conninfo, &err);
	if (opts == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid connection string for subscription %u: %s",
						subid, err ? err : "?")));
	initStringInfo(&buf);
	for (PQconninfoOption *o = opts; o->keyword; o++)
	{
		if (o->val == NULL || o->val[0] == '\0')
			continue;
		if (strcmp(o->keyword, "password") == 0 ||
			strcmp(o->keyword, "sslpassword") == 0)
			appendStringInfo(&buf, "%s%s='%s'", buf.len ? " " : "",
							 o->keyword, o->val);
	}
	PQconninfoFree(opts);
	if (buf.len == 0)
	{
		pfree(buf.data);
		return NULL;
	}
	return buf.data;
}


static void
do_connect(LrSourceState *state)
{
	List	   *pubs = NIL;
	const char *slot = kv_get(state->kv, "slot");
	const char *pub = kv_get(state->kv, "pub");
	const char *v;
	LrReport	rpt;
	ListCell   *lc;
	List	   *srcsegs = NIL;
	const char *secrets = NULL;
	List	   *repl_oids = NIL;
	XLogRecPtr	first_flush = InvalidXLogRecPtr;

	if (slot == NULL || pub == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("CONNECT payload lacks slot or pub")));

	/* a reconnect replaces whatever this process had */
	LrReaderDestroySub(state->spec.subid);
	state->readers = NIL;

	{
		char	   *copy = pstrdup(pub);
		char	   *saveptr = NULL;
		char	   *tok;

		for (tok = strtok_r(copy, ",", &saveptr); tok != NULL;
			 tok = strtok_r(NULL, ",", &saveptr))
			pubs = lappend(pubs, makeString(pstrdup(tok)));
	}

	/*
	 * srcrepl=<relid,...>: source relations that are DISTRIBUTED REPLICATED.
	 * Every source segment's stream carries their changes, so only the stream
	 * of source segment 0 emits them (apply_fill_queue).
	 */
	{
		List	   *lst = parse_int64_list(kv_get(state->kv, "srcrepl"));
		ListCell   *rc;

		foreach(rc, lst)
			repl_oids = lappend_oid(repl_oids, (Oid) *(int64 *) lfirst(rc));
	}

	v = kv_get_seg(state->kv, "srcsegs", GpIdentity.segindex);
	if (v != NULL && *v != '\0')
	{
		char	   *copy = pstrdup(v);
		char	   *saveptr = NULL;
		char	   *tok;

		for (tok = strtok_r(copy, ",", &saveptr); tok != NULL;
			 tok = strtok_r(NULL, ",", &saveptr))
		{
			int			seg = atoi(tok);

			if (seg >= 0)
				srcsegs = lappend_int(srcsegs, seg);
		}
	}

	/*
	 * The driver leaves the source's secrets out of the payload, because a
	 * server that logs statements logs their parameters too, and this payload
	 * is one.  The same conninfo is on this segment already:
	 * CREATE SUBSCRIPTION was dispatched here, and pg_subscription.subconninfo
	 * is readable by nobody but a superuser.
	 */
	{
		const char *sec = kv_get(state->kv, "secrets");

		secrets = (sec != NULL && atoi(sec) != 0) ?
			source_secrets(state->spec.subid) : NULL;
	}

	foreach(lc, srcsegs)
	{
		int			srcseg = lfirst_int(lc);
		const char *conninfo = kv_get_seg(state->kv, "conninfo", srcseg);
		LrReaderState *st;
		LrReaderStartPoint start;
		List	   *applied;

		if (conninfo == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CONNECT payload lacks conninfo for source segment %d",
							srcseg)));

		memset(&start, 0, sizeof(start));
		if ((v = kv_get_seg(state->kv, "flush", srcseg)) != NULL)
			start.seg_flush = parse_lsn(v);
		if ((v = kv_get_seg(state->kv, "pos", srcseg)) != NULL)
			start.seg_pos = parse_lsn(v);
		parse_local_snapshot(kv_get_seg(state->kv, "localsnap", srcseg), &start);
		parse_ds_cut(kv_get(state->kv, "dscut"), &start);
		applied = parse_int64_list(kv_get_seg(state->kv, "applied", srcseg));
		start.napplied = list_length(applied);
		if (start.napplied > 0)
		{
			int64	   *arr = palloc(sizeof(int64) * start.napplied);
			int			i = 0;
			ListCell   *ac;

			foreach(ac, applied)
				arr[i++] = *(int64 *) lfirst(ac);
			start.applied = arr;
		}

		st = LrReaderCreate(state->spec.subid, srcseg, state->spec.token, &start);
		state->readers = lappend(state->readers, st);
		if (first_flush == InvalidXLogRecPtr)
			first_flush = start.seg_flush;

		{
			const char *b = kv_get(state->kv, "binary");
			const char *sm = kv_get(state->kv, "streaming");
			const char *tp = kv_get(state->kv, "twophase");

			if (secrets != NULL)
				conninfo = psprintf("%s %s", conninfo, secrets);
			LrReaderConnect(st, conninfo, slot, pubs,
							b != NULL && atoi(b) != 0,
							sm == NULL || atoi(sm) != 0,
							tp == NULL || atoi(tp) != 0);
		}
		LrReaderSetReplicatedRels(st, repl_oids);
	}

	/*
	 * No source segment at all: still a live reader, with one state and no
	 * stream, so that the DRAIN and APPLY plans find it warm and the plans
	 * keep their shape whatever the source segment count is.
	 */
	if (state->readers == NIL)
	{
		LrReaderState *st = LrReaderCreate(state->spec.subid, -1,
										   state->spec.token, NULL);

		state->readers = lappend(state->readers, st);
		LrReaderSetReplicatedRels(st, repl_oids);
		elog(LOG, "MPP apply reader on segment %d has no source segment to stream from",
			 GpIdentity.segindex);
	}
	else if (list_length(state->readers) > 1)
		elog(LOG, "MPP apply reader on segment %d streams from %d source segments",
			 GpIdentity.segindex, list_length(state->readers));

	memset(&rpt, 0, sizeof(rpt));
	rpt.seg = GpIdentity.segindex;
	rpt.kind = LR_REPORT_CONNECTED;
	rpt.seg_lsn = first_flush;
	queue_report(state, &rpt);
}

/*
 * DRAIN one stream: release what the coordinator committed, read, report.
 *
 * Every report carries the SOURCE segment, which is what the driver indexes
 * its per-stream arrays by; only LR_REPORT_COLD names the reader itself,
 * because a cold reader has no stream to speak of.
 */
static void
drain_release_one(LrSourceState *state, LrReaderState *st)
{
	List	   *lst;
	ListCell   *lc;
	int			seg = LrReaderSourceSeg(st);
	const char *v;

	/* 1. release what the coordinator committed */
	lst = parse_int64_list(kv_get(state->kv, "release_gxid"));
	foreach(lc, lst)
	{
		LrTxn	   *txn = LrReaderFindGxid(st, *(int64 *) lfirst(lc));

		/*
		 * Absent on segments that did not take part.  Still open means it is
		 * being received: freeing it now would leave the rest of its stream
		 * with nowhere to go, so leave it and let it be released once it is
		 * complete.
		 */
		if (txn && txn->state != LR_TXN_OPEN)
			LrReaderRelease(st, txn);
	}
	if ((v = kv_get(state->kv, "release_local")) != NULL)
	{
		char	   *copy = pstrdup(v);
		char	   *saveptr = NULL;
		char	   *tok;

		for (tok = strtok_r(copy, ",", &saveptr); tok != NULL;
			 tok = strtok_r(NULL, ",", &saveptr))
		{
			char	   *colon = strchr(tok, ':');
			LrTxn	   *txn;

			if (colon == NULL)
				elog(ERROR, "malformed release_local entry \"%s\"", tok);
			if (atoi(tok) != seg)
				continue;
			txn = LrReaderFindLocal(st, parse_lsn(colon + 1));
			if (txn == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("local transaction %s to release is not buffered",
								colon + 1)));
			LrReaderRelease(st, txn);
		}
	}

}

/*
 * 2. read one stream -- unless the coordinator is holding it back.  It does
 * that when a reader has buffered more than it may (P0.5): not reading fills
 * this socket, which closes the window on the source, which leaves the
 * transactions in its WAL instead of in this segment's temporary files.
 * Everything else a DRAIN does still happens, and the feedback above all: a
 * source that hears nothing for wal_sender_timeout kills the walsender, and
 * the reconnection would re-deliver exactly the transactions that are
 * already held.
 */
static LrReceiveResult
drain_receive_one(LrReaderState *st, int64 gxid, int wait_ms, bool participant)
{
	if (wait_ms < 0)
		return LR_RECV_HELD;
	if (gxid != 0)
		return LrReaderReceive(st, gxid, participant ? wait_ms : 0, false);
	return LrReaderReceive(st, 0, wait_ms, true);
}

/*
 * 3. what this stream has to say, once the reading of every stream is done.
 * Reporting is separate from reading because a reader that holds several
 * streams reads them in rounds: one report per stream per DRAIN, whatever
 * the rounds did.
 */
static void
drain_report_one(LrSourceState *state, LrReaderState *st, int64 gxid,
				 LrReceiveResult res)
{
	ListCell   *lc;
	List	   *ready;
	LrReport	rpt;
	LrReaderStats stats;
	int			seg = LrReaderSourceSeg(st);

	/* feedback: what we received, what is safely applied */
	LrReaderSendFeedback(st);

	/* 4. reports */
	ready = LrReaderCollectReady(st);
	foreach(lc, ready)
	{
		LrReaderFillReport(st, (LrTxn *) lfirst(lc), seg, &rpt);
		queue_report(state, &rpt);
	}

	/*
	 * What is still on the wire.  A transaction the coordinator is waiting
	 * for may be arriving right now -- a large one takes far longer to
	 * stream than any grace period the coordinator could sensibly pick --
	 * and the coordinator cannot see that from here.  Say so, so that it
	 * waits for the transaction instead of concluding that this segment
	 * wrote nothing.
	 */
	{
		List	   *open = LrReaderOpenGxids(st);

		foreach(lc, open)
		{
			memset(&rpt, 0, sizeof(rpt));
			rpt.seg = seg;
			rpt.kind = LR_REPORT_OPEN;
			rpt.gxid = *(int64 *) lfirst(lc);
			queue_report(state, &rpt);
		}
		list_free_deep(open);
	}

	/* the target was applied before (replay after a restart): say so */
	if (gxid != 0 && LrReaderGxidStatus(st, gxid) == LR_GXID_APPLIED)
	{
		memset(&rpt, 0, sizeof(rpt));
		rpt.seg = seg;
		rpt.kind = LR_REPORT_DEDUP;
		rpt.gxid = gxid;
		queue_report(state, &rpt);
	}

	LrReaderGetStats(st, &stats);
	memset(&rpt, 0, sizeof(rpt));
	rpt.seg = seg;
	rpt.kind = LR_REPORT_STATUS;
	rpt.seg_lsn = stats.pos;
	rpt.gxid = (int64) stats.flush;
	rpt.nrows = stats.ntxns;
	rpt.nbytes = stats.held_bytes;
	queue_report(state, &rpt);

	if (res == LR_RECV_EOF)
		queue_cold(state);
}

static void
do_drain(LrSourceState *state)
{
	const char *v;
	int64		gxid = 0;
	int			wait_ms = LR_DRAIN_DEFAULT_WAIT_MS;
	List	   *parts = NIL;
	ListCell   *lc;
	TimestampTz started;
	LrReceiveResult *res;
	bool	   *part;
	int			nstreams;
	int			round;
	int			i;

	if (!reader_is_warm(state))
	{
		queue_cold(state);
		return;
	}

	if ((v = kv_get(state->kv, "gxid")) != NULL)
		gxid = strtoll(v, NULL, 10);
	if ((v = kv_get(state->kv, "wait_ms")) != NULL)
		wait_ms = atoi(v);
	if ((v = kv_get(state->kv, "parts")) != NULL)
		parts = parse_int64_list(v);	/* SOURCE content ids */

	started = GetCurrentTimestamp();

	/*
	 * The wait is a budget for the whole DRAIN, not for each stream: a reader
	 * that serves several source segments may not multiply the coordinator's
	 * grace period by how many it happens to hold.  Spending it in order,
	 * though, is the same thing as starving every stream but the first: the
	 * first takes the whole budget and the rest are left with nothing to wait
	 * with, so they only ever take what the socket already holds.  With a
	 * three-segment source and a two-segment target that is one stream in
	 * three, permanently, and it showed: its source sat at two thirds of the
	 * others' CPU and finished last.
	 *
	 * So read in rounds instead.  The first round waits for nothing and takes
	 * what every stream already has; later rounds split what is left of the
	 * budget between the streams still worth waiting on, so each waits on its
	 * own socket for a share.  Reports come after, one per stream.
	 */
	nstreams = 0;
	foreach(lc, state->readers)
		if (LrReaderHasSource((LrReaderState *) lfirst(lc)))
			nstreams++;

	res = palloc(sizeof(LrReceiveResult) * Max(1, list_length(state->readers)));
	part = palloc(sizeof(bool) * Max(1, list_length(state->readers)));
	i = 0;
	foreach(lc, state->readers)
	{
		LrReaderState *st = (LrReaderState *) lfirst(lc);
		ListCell   *pc;

		part[i] = (parts == NIL);
		foreach(pc, parts)
			if (*(int64 *) lfirst(pc) == LrReaderSourceSeg(st))
				part[i] = true;
		res[i] = LR_RECV_DRAINED;
		if (LrReaderHasSource(st))
			drain_release_one(state, st);
		i++;
	}

	for (round = 0; nstreams > 0; round++)
	{
		int			share;
		bool		all_ready = true;

		if (wait_ms <= 0)
			share = wait_ms;	/* held (-1), or asked not to wait at all */
		else if (round == 0)
			share = 0;			/* take what is there before anyone waits */
		else
		{
			int			left = Max(0, wait_ms -
									  (int) TimestampDifferenceMilliseconds(started,
																			GetCurrentTimestamp()));

			if (left <= 0)
				break;
			share = Max(1, left / nstreams);
		}

		i = 0;
		foreach(lc, state->readers)
		{
			LrReaderState *st = (LrReaderState *) lfirst(lc);

			if (LrReaderHasSource(st))
			{
				res[i] = drain_receive_one(st, gxid, share, part[i]);

				/*
				 * DRAINED is "the socket is empty just now", not "there is
				 * nothing to wait for": only what this DRAIN was asked for
				 * -- the target, or anything at all on a long poll -- ends
				 * the rounds early.
				 */
				if (part[i] &&
					res[i] != LR_RECV_TARGET_READY &&
					res[i] != LR_RECV_EOF &&
					res[i] != LR_RECV_HELD)
					all_ready = false;
			}
			i++;
		}
		if (wait_ms <= 0 || all_ready)
			break;
	}

	i = 0;
	foreach(lc, state->readers)
	{
		LrReaderState *st = (LrReaderState *) lfirst(lc);

		/* no source segment: nothing to release, read or report but the state */
		if (!LrReaderHasSource(st))
		{
			LrReport	rpt;

			memset(&rpt, 0, sizeof(rpt));
			rpt.seg = -1;
			rpt.kind = LR_REPORT_STATUS;
			queue_report(state, &rpt);
		}
		else
			drain_report_one(state, st, gxid, res[i]);
		i++;
	}
	pfree(res);
	pfree(part);
}

static void
apply_begin(LrSourceState *state)
{
	const char *v;
	char	   *copy,
			   *saveptr = NULL,
			   *tok;

	if (!reader_is_warm(state))
	{
		queue_cold(state);
		return;
	}

	/*
	 * txns=<source seg>:<gxid>,<source seg>:L<lsn>,...  Keep the entries of
	 * the streams this reader holds, in order; the rest belong to another
	 * reader.
	 */
	v = kv_get(state->kv, "txns");
	copy = pstrdup(v ? v : "");
	for (tok = strtok_r(copy, ",", &saveptr); tok != NULL;
		 tok = strtok_r(NULL, ",", &saveptr))
	{
		char	   *colon = strchr(tok, ':');
		LrApplyItem *item;
		LrReaderState *st = NULL;
		ListCell   *lc;
		int			seg;

		if (colon == NULL)
			elog(ERROR, "malformed txns entry \"%s\"", tok);
		seg = atoi(tok);
		foreach(lc, state->readers)
			if (LrReaderSourceSeg((LrReaderState *) lfirst(lc)) == seg)
				st = (LrReaderState *) lfirst(lc);
		if (st == NULL)
			continue;
		item = palloc0(sizeof(LrApplyItem));
		item->reader = st;
		if (colon[1] == 'L')
			item->local_lsn = parse_lsn(colon + 2);
		else
			item->gxid = strtoll(colon + 1, NULL, 10);

		/*
		 * ":<relid>.<relid>..." after the identity: relations whose copy
		 * already holds this transaction's rows, for this transaction alone.
		 */
		{
			char	   *skip = strchr(colon + 1, ':');

			if (skip != NULL && skip[1] != '\0')
			{
				char	   *rest = skip + 1;
				int			n = 1;

				for (char *c = rest; *c; c++)
					if (*c == '.')
						n++;
				item->skip = palloc(sizeof(Oid) * n);
				for (char *c = rest; c != NULL && *c;)
				{
					char	   *dot = strchr(c, '.');

					item->skip[item->nskip++] = (Oid) strtoul(c, NULL, 10);
					c = dot ? dot + 1 : NULL;
				}
			}
		}
		state->items = lappend(state->items, item);
	}
	state->item_next = list_head(state->items);

	/*
	 * defer=<remote relids>  skip these tables, keep their transactions
	 * catchup=<remote relids>  emit only these tables (after their COPY)
	 */
	if ((v = kv_get(state->kv, "catchup")) != NULL && *v != '\0')
		state->filter_only = true;
	else
		v = kv_get(state->kv, "defer");
	if (v != NULL && *v != '\0')
	{
		List	   *lst = parse_int64_list(v);
		ListCell   *lc;

		state->filter_relids = palloc(sizeof(Oid) * list_length(lst));
		foreach(lc, lst)
			state->filter_relids[state->nfilter++] = (Oid) *(int64 *) lfirst(lc);
	}

	state->router = LrRouterCreate(state->plancxt, state->spec.nsegs,
								   GpIdentity.segindex);
}

/*
 * Is this table one whose changes this plan carries?  Outside a catch-up
 * plan every table but the deferred ones; in a catch-up plan only them.
 */
static bool
source_emits_rel(LrSourceState *state, Oid relid)
{
	/*
	 * What the open transaction alone may not write.  This is not the plan's
	 * filter below but the coordinator's verdict on this one transaction:
	 * whatever holds the relation has its rows already, which for two
	 * transactions in the same unit can differ.
	 */
	if (state->cur_item != NULL)
		for (int i = 0; i < state->cur_item->nskip; i++)
			if (state->cur_item->skip[i] == relid)
				return false;

	for (int i = 0; i < state->nfilter; i++)
		if (state->filter_relids[i] == relid)
			return state->filter_only;
	return !state->filter_only;
}

/*
 * A TRUNCATE record names several relations at once (proto.c: int32 count,
 * int8 flags, int32 relids[]).  Keep only the ones this plan carries; the
 * rest are truncated by the plan that does carry them.  Returns false when
 * nothing is left.
 */
static bool
truncate_filter(LrSourceState *state, const char *data, int len,
				StringInfo out)
{
	uint32		nrelids;
	uint32		kept = 0;
	uint8		flags;
	const char *p = data;
	Oid		   *ids;

	if (len < 5)
		elog(ERROR, "malformed TRUNCATE record of %d bytes", len);
	memcpy(&nrelids, p, 4);
	nrelids = pg_ntoh32(nrelids);
	p += 4;
	flags = (uint8) *p++;
	if (len != 5 + (int) nrelids * 4)
		elog(ERROR, "malformed TRUNCATE record: %u relations in %d bytes",
			 nrelids, len);

	ids = palloc(sizeof(Oid) * Max(nrelids, 1));
	for (uint32 i = 0; i < nrelids; i++)
	{
		uint32		id;

		memcpy(&id, p, 4);
		p += 4;
		id = pg_ntoh32(id);
		if (source_emits_rel(state, (Oid) id))
			ids[kept++] = (Oid) id;
	}
	if (kept == 0)
	{
		pfree(ids);
		return false;
	}

	initStringInfo(out);
	pq_sendint32(out, kept);
	pq_sendint8(out, flags);
	for (uint32 i = 0; i < kept; i++)
		pq_sendint32(out, ids[i]);
	pfree(ids);
	return true;
}

/*
 * Queue the rows of the next record.  Returns false when every listed
 * transaction has been emitted.
 */
static bool
apply_fill_queue(LrSourceState *state)
{
	LrReaderState *st = state->cur_reader;

	for (;;)
	{
		char		op;
		Oid			relid;
		const char *data;
		int			len;

		if (!state->cur_open)
		{
			LrApplyItem *item;

			if (state->item_next == NULL)
				return false;
			item = (LrApplyItem *) lfirst(state->item_next);
			state->item_next = lnext(state->items, state->item_next);

			st = state->cur_reader = item->reader;
			state->cur_item = item;
			if (item->gxid != 0)
				state->cur_txn = LrReaderFindGxid(st, item->gxid);
			else
				state->cur_txn = LrReaderFindLocal(st, item->local_lsn);
			if (state->cur_txn == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("transaction " INT64_FORMAT " / %X/%X to apply is not buffered for source segment %d",
								item->gxid, LSN_FORMAT_ARGS(item->local_lsn),
								LrReaderSourceSeg(st))));
			LrChangesBegin(state->cur_txn, &state->cur);
			state->cur_open = true;
		}

		if (!LrChangesNext(&state->cur, &op, &relid, &data, &len))
		{
			LrChangesEnd(&state->cur);
			state->cur_open = false;
			continue;
		}

		switch (op)
		{
			case LR_OP_RELATION:
				/* every writer needs these */
				if (!source_emits_rel(state, relid))
					break;
				for (int seg = 0; seg < state->spec.nsegs; seg++)
					queue_row(state, seg, relid, op, data, len);
				return true;

			case LR_OP_TRUNCATE:
				{
					StringInfoData body;

					if (state->nfilter > 0)
					{
						if (!truncate_filter(state, data, len, &body))
							break;
						for (int seg = 0; seg < state->spec.nsegs; seg++)
							queue_row(state, seg, InvalidOid, op, body.data, body.len);
						pfree(body.data);
					}
					else
						for (int seg = 0; seg < state->spec.nsegs; seg++)
							queue_row(state, seg, InvalidOid, op, data, len);
					return true;
				}

			case LR_OP_INSERT:
			case LR_OP_UPDATE:
			case LR_OP_DELETE:
				{
					const LogicalRepRelation *remoterel;
					LrRoutedRow rows[2];
					int			n;

					if (!source_emits_rel(state, relid))
						break;

					/*
					 * A replicated source relation has the same row on every
					 * source segment, so every stream carries this change;
					 * the reader of source segment 0 emits it (and the router
					 * broadcasts it or hashes it once), the others drop it.
					 */
					if (LrReaderRelIsReplicated(st, relid) &&
						LrReaderSourceSeg(st) != 0)
						break;
					remoterel = LrReaderRemoteRel(st, relid);
					if (remoterel == NULL)
						elog(ERROR, "no description for relation %u", relid);
					n = LrRouteChange(state->router, remoterel,
									  LrReaderRelGeneration(st, relid), op,
									  data, len, rows);
					for (int i = 0; i < n; i++)
					{
						if (rows[i].target >= 0)
							queue_row(state, rows[i].target, relid, rows[i].op,
									  rows[i].data, rows[i].len);
						else
							for (int seg = 0; seg < state->spec.nsegs; seg++)
								queue_row(state, seg, relid, rows[i].op,
										  rows[i].data, rows[i].len);
					}
					return true;
				}

			default:
				/* B/E and anything else the reader stored: not forwarded */
				break;
		}
	}
}

static void
apply_finish(LrSourceState *state)
{
	if (state->cur_open)
	{
		LrChangesEnd(&state->cur);
		state->cur_open = false;
	}
	if (state->router)
	{
		LrRouterClose(state->router);
		state->router = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Output
 * ------------------------------------------------------------------------
 */

static void
queue_row(LrSourceState *state, int32 target, Oid relid, char op,
		  const char *data, int len)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(state->plancxt);
	LrOutRow   *row = palloc(sizeof(LrOutRow));

	row->target_segid = target;
	row->relid = relid;
	row->op = op;
	row->data = palloc(len);
	memcpy(row->data, data, len);
	row->len = len;
	state->queue = lappend(state->queue, row);
	state->since_sip++;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Reports are addressed to the writer on our own segment so that they never
 * cross the network; LogicalApply passes them through.
 */
static void
queue_report(LrSourceState *state, const LrReport *rpt)
{
	StringInfoData buf;

	initStringInfo(&buf);
	LrReportSerialize(&buf, rpt);
	queue_row(state, GpIdentity.segindex % state->spec.nsegs, InvalidOid,
			  rpt->kind, buf.data, buf.len);
	pfree(buf.data);
}

/*
 * Emit one row on the source channel.  The payload is allocated in the
 * per-tuple memory context.
 */
static TupleTableSlot *
source_store_row(LrSourceState *state, int32 target_segid, Oid relid,
				 char op, const char *payload, int len)
{
	TupleTableSlot *slot = state->css.ss.ss_ScanTupleSlot;
	MemoryContext oldcxt;
	bytea	   *b;

	ExecClearTuple(slot);
	oldcxt = MemoryContextSwitchTo(state->css.ss.ps.ps_ExprContext->ecxt_per_tuple_memory);
	b = (bytea *) palloc(VARHDRSZ + len);
	SET_VARSIZE(b, VARHDRSZ + len);
	memcpy(VARDATA(b), payload, len);
	MemoryContextSwitchTo(oldcxt);

	slot->tts_values[LR_SRC_ATT_TARGET_SEGID - 1] = Int32GetDatum(target_segid);
	slot->tts_values[LR_SRC_ATT_RELID - 1] = ObjectIdGetDatum(relid);
	slot->tts_values[LR_SRC_ATT_OP - 1] = CharGetDatum(op);
	slot->tts_values[LR_SRC_ATT_PAYLOAD - 1] = PointerGetDatum(b);
	slot->tts_values[LR_SRC_ATT_SEQ - 1] =
		Int64GetDatum(((int64) GpIdentity.segindex << 48) | state->nseq);
	memset(slot->tts_isnull, 0, sizeof(bool) * LR_SRC_NATTS);
	state->nseq++;

	return ExecStoreVirtualTuple(slot);
}

/* ------------------------------------------------------------------------
 * Payload parsing: "key=value" lines
 * ------------------------------------------------------------------------
 */

static List *
parse_kv(const char *payload)
{
	List	   *kv = NIL;
	char	   *copy = pstrdup(payload);
	char	   *saveptr = NULL;
	char	   *line;

	for (line = strtok_r(copy, "\n", &saveptr); line != NULL;
		 line = strtok_r(NULL, "\n", &saveptr))
	{
		char	   *eq = strchr(line, '=');
		LrKV	   *e;

		if (*line == '\0')
			continue;
		if (eq == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("malformed payload line \"%s\"", line)));
		e = palloc(sizeof(LrKV));
		*eq = '\0';
		e->key = line;
		e->value = eq + 1;
		kv = lappend(kv, e);
	}
	return kv;
}

static const char *
kv_get(List *kv, const char *key)
{
	ListCell   *lc;

	foreach(lc, kv)
	{
		LrKV	   *e = (LrKV *) lfirst(lc);

		if (strcmp(e->key, key) == 0)
			return e->value;
	}
	return NULL;
}

static const char *
kv_get_seg(List *kv, const char *key, int seg)
{
	char		buf[64];

	snprintf(buf, sizeof(buf), "%s.%d", key, seg);
	return kv_get(kv, buf);
}

/* comma-separated integers; NULL or empty gives NIL */
/*
 * "xmin:xmax:xip,xip,..." -- txid_current_snapshot()'s own text, as the cut
 * point recorded it for this source segment.  The values are epoch-extended;
 * the stream carries plain 32-bit xids, so the low half is what is compared,
 * which is right for as long as the filter matters (it stops matching a
 * wraparound away, and stops mattering long before that).
 */
/*
 * "xmax:g1,g2,..." -- the distributed half of the cut point, the same for
 * every source segment.  The in-progress list is short: it holds only the
 * distributed transactions that were still running at the cut, and it stops
 * mattering shortly after the synchronisation ends, when everything arriving
 * is at or above xmax and falls out on the first test.
 */
static void
parse_ds_cut(const char *value, LrReaderStartPoint *start)
{
	char	   *copy,
			   *saveptr = NULL,
			   *tok,
			   *rest;
	List	   *gxids = NIL;
	ListCell   *lc;
	int64	   *arr;
	int			i = 0;

	if (value == NULL || *value == '\0')
		return;
	copy = pstrdup(value);
	tok = strtok_r(copy, ":", &saveptr);
	if (tok == NULL)
		return;
	start->ds_xmax = strtoll(tok, NULL, 10);
	if (start->ds_xmax == 0)
		return;

	rest = strtok_r(NULL, ":", &saveptr);
	for (tok = rest ? strtok_r(rest, ",", &saveptr) : NULL; tok != NULL;
		 tok = strtok_r(NULL, ",", &saveptr))
	{
		int64	   *v = palloc(sizeof(int64));

		*v = strtoll(tok, NULL, 10);
		gxids = lappend(gxids, v);
	}
	start->nds_inprogress = list_length(gxids);
	if (start->nds_inprogress > 0)
	{
		arr = palloc(sizeof(int64) * start->nds_inprogress);
		foreach(lc, gxids)
			arr[i++] = *(int64 *) lfirst(lc);
		start->ds_inprogress = arr;
	}
}

static void
parse_local_snapshot(const char *value, LrReaderStartPoint *start)
{
	char	   *copy,
			   *xip,
			   *saveptr = NULL,
			   *tok;
	List	   *xids = NIL;
	ListCell   *lc;
	TransactionId *arr;
	int			i = 0;

	if (value == NULL || *value == '\0')
		return;
	copy = pstrdup(value);
	tok = strtok_r(copy, ":", &saveptr);
	if (tok == NULL)
		return;
	start->sync_xmin = (TransactionId) strtoull(tok, NULL, 10);
	tok = strtok_r(NULL, ":", &saveptr);
	if (tok == NULL)
	{
		start->sync_xmin = InvalidTransactionId;
		return;
	}
	start->sync_xmax = (TransactionId) strtoull(tok, NULL, 10);

	/* the rest of the string, if any, is the in-flight list */
	xip = strtok_r(NULL, ":", &saveptr);
	for (tok = xip ? strtok_r(xip, ",", &saveptr) : NULL; tok != NULL;
		 tok = strtok_r(NULL, ",", &saveptr))
	{
		TransactionId *v = palloc(sizeof(TransactionId));

		*v = (TransactionId) strtoull(tok, NULL, 10);
		xids = lappend(xids, v);
	}
	start->nsync_xip = list_length(xids);
	if (start->nsync_xip == 0)
		return;
	arr = palloc(sizeof(TransactionId) * start->nsync_xip);
	foreach(lc, xids)
		arr[i++] = *(TransactionId *) lfirst(lc);
	start->sync_xip = arr;
}

static List *
parse_int64_list(const char *value)
{
	List	   *result = NIL;
	char	   *copy,
			   *saveptr = NULL,
			   *tok;

	if (value == NULL || *value == '\0')
		return NIL;
	copy = pstrdup(value);
	for (tok = strtok_r(copy, ",", &saveptr); tok != NULL;
		 tok = strtok_r(NULL, ",", &saveptr))
	{
		int64	   *v = palloc(sizeof(int64));

		*v = strtoll(tok, NULL, 10);
		result = lappend(result, v);
	}
	return result;
}

static XLogRecPtr
parse_lsn(const char *value)
{
	bool		have_error = false;
	XLogRecPtr	lsn;

	if (value == NULL || *value == '\0' || strcmp(value, "0") == 0)
		return InvalidXLogRecPtr;
	/* the reports carry LSNs as plain integers; accept both spellings */
	if (strchr(value, '/') == NULL)
	{
		char	   *end;
		unsigned long long v = strtoull(value, &end, 10);

		if (*end != '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid LSN \"%s\" in payload", value)));
		return (XLogRecPtr) v;
	}
	lsn = pg_lsn_in_internal(value, &have_error);
	if (have_error)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid LSN \"%s\" in payload", value)));
	return lsn;
}
