/*-------------------------------------------------------------------------
 *
 * mpp_apply_plan.c
 *	  Hand-built plans and operators for MPP logical replication apply.
 *
 * See contrib/mpp_logicalrep/mpp_apply.h for the plan shape.  This file
 * owns:
 *
 *   - registration of the two CustomScan providers;
 *   - the plan builder, which produces a PlannedStmt plus slice table
 *     without going through the planner;
 *   - the runner, which executes such a plan from a QD backend through the
 *     normal ExecutorStart/Run dispatch path and collects the report rows
 *     that come back through the Gather Motion;
 *   - the report row format shared by readers, writers and the driver.
 *
 * The operators live in their own files: mpp_source.c (LogicalSource,
 * reader gang) and mpp_writer.c (LogicalApply, writer gang).  This file
 * owns the plan shape and everything both operators share.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_apply_plan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "executor/execdesc.h"
#include "executor/tstoreReceiver.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/value.h"
#include "replication/logicalrelation.h"
#include "mpp_apply.h"
#include "tcop/dest.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "access/xact.h"
#include "utils/snapmgr.h"

/* Plan node ids, only need to be unique within the plan */
#define LR_NODEID_GATHER	1
#define LR_NODEID_APPLY		2
#define LR_NODEID_EXPLICIT	3
#define LR_NODEID_SOURCE	4

/* ----------------------------------------------------------------
 * Common helpers
 * ----------------------------------------------------------------
 */

const char *
LrPlanModeName(LrPlanMode mode)
{
	switch (mode)
	{
		case LR_PLAN_CONNECT:
			return "CONNECT";
		case LR_PLAN_DRAIN:
			return "DRAIN";
		case LR_PLAN_APPLY:
			return "APPLY";
	}
	return "???";
}

TupleDesc
LrSourceTupleDesc(void)
{
	TupleDesc	desc = CreateTemplateTupleDesc(LR_SRC_NATTS);

	TupleDescInitEntry(desc, LR_SRC_ATT_TARGET_SEGID, "target_segid", INT4OID, -1, 0);
	TupleDescInitEntry(desc, LR_SRC_ATT_RELID, "relid", OIDOID, -1, 0);
	TupleDescInitEntry(desc, LR_SRC_ATT_OP, "op", CHAROID, -1, 0);
	TupleDescInitEntry(desc, LR_SRC_ATT_PAYLOAD, "payload", BYTEAOID, -1, 0);
	TupleDescInitEntry(desc, LR_SRC_ATT_SEQ, "seq", INT8OID, -1, 0);
	return desc;
}

TupleDesc
LrReportTupleDesc(void)
{
	TupleDesc	desc = CreateTemplateTupleDesc(LR_RPT_NATTS);

	TupleDescInitEntry(desc, LR_RPT_ATT_SEG, "seg", INT4OID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_KIND, "kind", CHAROID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_GXID, "gxid", INT8OID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_SEG_LSN, "seg_lsn", INT8OID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_REMOTE_RELIDS, "remote_relids", OIDARRAYOID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_REL_NAMES, "rel_names", TEXTARRAYOID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_LOCKMODE, "lockmode", INT2OID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_NROWS, "nrows", INT8OID, -1, 0);
	TupleDescInitEntry(desc, LR_RPT_ATT_NBYTES, "nbytes", INT8OID, -1, 0);
	return desc;
}

/*
 * Wire format of a report, used as the payload of report rows.
 */
void
LrReportSerialize(StringInfo buf, const LrReport *rpt)
{
	ListCell   *lc;

	pq_sendint32(buf, rpt->seg);
	pq_sendbyte(buf, rpt->kind);
	pq_sendint64(buf, rpt->gxid);
	pq_sendint64(buf, rpt->seg_lsn);
	pq_sendint16(buf, rpt->lockmode);
	pq_sendint64(buf, rpt->nrows);
	pq_sendint64(buf, rpt->nbytes);
	pq_sendint32(buf, list_length(rpt->remote_relids));
	foreach(lc, rpt->remote_relids)
		pq_sendint32(buf, lfirst_oid(lc));
	pq_sendint32(buf, list_length(rpt->rel_names));
	foreach(lc, rpt->rel_names)
		pq_sendstring(buf, (const char *) lfirst(lc));
}

void
LrReportDeserialize(const char *data, int len, LrReport *rpt)
{
	StringInfoData buf;
	int			n;

	buf.data = unconstify(char *, data);
	buf.len = len;
	buf.maxlen = len;
	buf.cursor = 0;

	memset(rpt, 0, sizeof(*rpt));
	rpt->seg = pq_getmsgint(&buf, 4);
	rpt->kind = pq_getmsgbyte(&buf);
	rpt->gxid = pq_getmsgint64(&buf);
	rpt->seg_lsn = pq_getmsgint64(&buf);
	rpt->lockmode = pq_getmsgint(&buf, 2);
	rpt->nrows = pq_getmsgint64(&buf);
	rpt->nbytes = pq_getmsgint64(&buf);
	n = pq_getmsgint(&buf, 4);
	while (n-- > 0)
		rpt->remote_relids = lappend_oid(rpt->remote_relids, pq_getmsgint(&buf, 4));
	n = pq_getmsgint(&buf, 4);
	while (n-- > 0)
		rpt->rel_names = lappend(rpt->rel_names, pstrdup(pq_getmsgstring(&buf)));
	pq_getmsgend(&buf);
}

/*
 * Fill a values/isnull pair laid out as LrReportTupleDesc().  Arrays are
 * allocated in CurrentMemoryContext.
 */
void
LrReportToValues(const LrReport *rpt, Datum *values, bool *isnull)
{
	int			n;
	Datum	   *elems;
	ListCell   *lc;
	int			i;

	memset(isnull, 0, sizeof(bool) * LR_RPT_NATTS);

	values[LR_RPT_ATT_SEG - 1] = Int32GetDatum(rpt->seg);
	values[LR_RPT_ATT_KIND - 1] = CharGetDatum(rpt->kind);
	values[LR_RPT_ATT_GXID - 1] = Int64GetDatum(rpt->gxid);
	values[LR_RPT_ATT_SEG_LSN - 1] = Int64GetDatum(rpt->seg_lsn);

	n = list_length(rpt->remote_relids);
	elems = palloc(sizeof(Datum) * Max(n, 1));
	i = 0;
	foreach(lc, rpt->remote_relids)
		elems[i++] = ObjectIdGetDatum(lfirst_oid(lc));
	values[LR_RPT_ATT_REMOTE_RELIDS - 1] =
		PointerGetDatum(construct_array_builtin(elems, n, OIDOID));

	n = list_length(rpt->rel_names);
	elems = palloc(sizeof(Datum) * Max(n, 1));
	i = 0;
	foreach(lc, rpt->rel_names)
		elems[i++] = PointerGetDatum(cstring_to_text((const char *) lfirst(lc)));
	values[LR_RPT_ATT_REL_NAMES - 1] =
		PointerGetDatum(construct_array_builtin(elems, n, TEXTOID));

	values[LR_RPT_ATT_LOCKMODE - 1] = Int16GetDatum(rpt->lockmode);
	values[LR_RPT_ATT_NROWS - 1] = Int64GetDatum(rpt->nrows);
	values[LR_RPT_ATT_NBYTES - 1] = Int64GetDatum(rpt->nbytes);
}

/*
 * The spec travels in custom_private as one String node:
 *   "<mode> <subid> <token> <nsegs>|<payload>"
 */
static char *
spec_serialize(const LrPlanSpec *spec)
{
	return psprintf("%d %u " INT64_FORMAT " %d|%s",
					(int) spec->mode, spec->subid, spec->token, spec->nsegs,
					spec->payload ? spec->payload : "");
}

void
LrSpecFromCustomScan(const CustomScan *cscan, LrPlanSpec *spec)
{
	const char *str;
	char	   *end;
	const char *bar;

	if (list_length(cscan->custom_private) != 1 ||
		!IsA(linitial(cscan->custom_private), String))
		elog(ERROR, "malformed %s spec", cscan->methods->CustomName);

	str = strVal(linitial(cscan->custom_private));
	memset(spec, 0, sizeof(*spec));

	spec->mode = (LrPlanMode) strtol(str, &end, 10);
	spec->subid = (Oid) strtoul(end, &end, 10);
	spec->token = strtoll(end, &end, 10);
	spec->nsegs = (int) strtol(end, &end, 10);
	bar = strchr(end, '|');
	if (bar == NULL)
		elog(ERROR, "malformed %s spec: \"%s\"", cscan->methods->CustomName, str);
	spec->payload = bar[1] ? pstrdup(bar + 1) : NULL;
}

/*
 * Does this process execute the slice that is currently being initialized?
 * ExecInitMotion sets estate->currentSliceId to the sending slice while it
 * initializes its subtree, so during BeginCustomScan it names our slice.
 */
bool
LrSliceIsLocal(EState *estate)
{
	return Gp_role == GP_ROLE_EXECUTE &&
		estate->currentSliceId == LocallyExecutingSliceIndex(estate);
}

void
LrExplainSpec(const LrPlanSpec *spec, ExplainState *es)
{
	ExplainPropertyText("Mode", LrPlanModeName(spec->mode), es);
	ExplainPropertyInteger("Subscription", NULL, spec->subid, es);
	if (es->verbose)
	{
		ExplainPropertyInteger("Token", NULL, spec->token, es);
		ExplainPropertyInteger("Gang Size", NULL, spec->nsegs, es);
		if (spec->payload)
			ExplainPropertyText("Payload", spec->payload, es);
	}
}

/*
 * Store a report into the operator's scan slot.
 */
TupleTableSlot *
LrStoreReport(ScanState *ss, const LrReport *rpt)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	MemoryContext oldcxt;

	ExecClearTuple(slot);
	oldcxt = MemoryContextSwitchTo(ss->ps.ps_ExprContext->ecxt_per_tuple_memory);
	LrReportToValues(rpt, slot->tts_values, slot->tts_isnull);
	MemoryContextSwitchTo(oldcxt);
	return ExecStoreVirtualTuple(slot);
}

bool
LrScanRecheck(ScanState *ss, TupleTableSlot *slot)
{
	return true;
}

void
LrRegisterCustomScanMethods(void)
{
	static bool registered = false;

	if (registered)
		return;
	RegisterCustomScanMethods(&LrSourceScanMethods);
	RegisterCustomScanMethods(&LrApplyScanMethods);
	registered = true;
}

/* ----------------------------------------------------------------
 * Plan builder
 * ----------------------------------------------------------------
 */

/*
 * Target list of Vars referencing varno, one per attribute of desc.
 */
static List *
tlist_of_vars(TupleDesc desc, Index varno)
{
	List	   *tlist = NIL;

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		Var		   *var = makeVar(varno, i + 1, att->atttypid, att->atttypmod,
								  att->attcollation, 0);

		tlist = lappend(tlist, makeTargetEntry((Expr *) var, i + 1,
											   pstrdup(NameStr(att->attname)),
											   false));
	}
	return tlist;
}

/*
 * custom_scan_tlist: only the types matter (ExecTypeFromTL builds the scan
 * tuple descriptor from it), so null Consts of the right types will do.
 */
static List *
tlist_of_types(TupleDesc desc)
{
	List	   *tlist = NIL;

	for (int i = 0; i < desc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(desc, i);
		Const	   *c = makeConst(att->atttypid, att->atttypmod, att->attcollation,
								  att->attlen, (Datum) 0, true, att->attbyval);

		tlist = lappend(tlist, makeTargetEntry((Expr *) c, i + 1,
											   pstrdup(NameStr(att->attname)),
											   false));
	}
	return tlist;
}

static CustomScan *
make_operator(const CustomScanMethods *methods, TupleDesc outdesc,
			  List *children, const char *specstr, int plan_node_id,
			  double rows)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->scan.scanrelid = 0;
	cscan->scan.plan.targetlist = tlist_of_vars(outdesc, INDEX_VAR);
	cscan->scan.plan.plan_node_id = plan_node_id;
	cscan->scan.plan.plan_rows = rows;
	cscan->flags = 0;
	cscan->custom_plans = children;
	cscan->custom_private = list_make1(makeString(pstrdup(specstr)));
	cscan->custom_scan_tlist = tlist_of_types(outdesc);
	cscan->methods = methods;
	return cscan;
}

static Motion *
make_motion_node(MotionType type, Plan *child, TupleDesc desc, int motionID,
				 AttrNumber segidColIdx, int plan_node_id)
{
	Motion	   *motion = makeNode(Motion);

	motion->plan.lefttree = child;
	motion->plan.targetlist = tlist_of_vars(desc, OUTER_VAR);
	motion->plan.plan_node_id = plan_node_id;
	motion->plan.plan_rows = child->plan_rows;
	motion->motionType = type;
	motion->sendSorted = false;
	motion->motionID = motionID;
	motion->segidColIdx = segidColIdx;
	return motion;
}

PlannedStmt *
LrBuildApplyPlan(const LrPlanSpec *spec)
{
	char	   *specstr = spec_serialize(spec);
	TupleDesc	srcdesc = LrSourceTupleDesc();
	TupleDesc	rptdesc = LrReportTupleDesc();
	CustomScan *source;
	Motion	   *explicit;
	CustomScan *apply;
	Motion	   *gather;
	PlanSlice  *slices;
	PlannedStmt *pstmt;

	if (spec->nsegs <= 0)
		elog(ERROR, "LrBuildApplyPlan: invalid gang size %d", spec->nsegs);

	/* slice 2: LogicalSource on the reader gang */
	source = make_operator(&LrSourceScanMethods, srcdesc, NIL, specstr,
						   LR_NODEID_SOURCE, spec->nsegs);

	/* Explicit Redistribute Motion, routed by target_segid */
	explicit = make_motion_node(MOTIONTYPE_EXPLICIT, (Plan *) source, srcdesc,
								LR_MOTION_EXPLICIT, LR_SRC_ATT_TARGET_SEGID,
								LR_NODEID_EXPLICIT);

	/* slice 1: LogicalApply on the writer gang, child = the motion */
	apply = make_operator(&LrApplyScanMethods, rptdesc,
						  list_make1(explicit), specstr,
						  LR_NODEID_APPLY, spec->nsegs);

	/* slice 0: Gather Motion to the QD */
	gather = make_motion_node(MOTIONTYPE_GATHER, (Plan *) apply, rptdesc,
							  LR_MOTION_GATHER, 0, LR_NODEID_GATHER);

	slices = palloc0(3 * sizeof(PlanSlice));
	slices[LR_SLICE_QD].sliceIndex = LR_SLICE_QD;
	slices[LR_SLICE_QD].parentIndex = -1;
	slices[LR_SLICE_QD].gangType = GANGTYPE_UNALLOCATED;
	slices[LR_SLICE_QD].numsegments = 0;
	slices[LR_SLICE_QD].segindex = -1;

	slices[LR_SLICE_WRITER].sliceIndex = LR_SLICE_WRITER;
	slices[LR_SLICE_WRITER].parentIndex = LR_SLICE_QD;
	slices[LR_SLICE_WRITER].gangType = GANGTYPE_PRIMARY_WRITER;
	slices[LR_SLICE_WRITER].numsegments = spec->nsegs;
	slices[LR_SLICE_WRITER].segindex = -1;

	slices[LR_SLICE_READER].sliceIndex = LR_SLICE_READER;
	slices[LR_SLICE_READER].parentIndex = LR_SLICE_WRITER;
	slices[LR_SLICE_READER].gangType = GANGTYPE_PRIMARY_READER;
	slices[LR_SLICE_READER].numsegments = spec->nsegs;
	slices[LR_SLICE_READER].segindex = -1;

	pstmt = makeNode(PlannedStmt);
	pstmt->commandType = CMD_SELECT;
	pstmt->queryId = 0;
	pstmt->canSetTag = true;
	pstmt->planTree = (Plan *) gather;
	pstmt->rtable = NIL;
	pstmt->planGen = PLANGEN_PLANNER;
	pstmt->numSlices = 3;
	pstmt->slices = slices;

	return pstmt;
}

/* ----------------------------------------------------------------
 * Runner
 * ----------------------------------------------------------------
 */

static QueryDesc *
start_plan(PlannedStmt *pstmt, const char *sourceText, DestReceiver *dest,
		   int eflags, bool *pushed_snapshot)
{
	QueryDesc  *qdesc;

	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("MPP apply plans must run on the coordinator")));

	*pushed_snapshot = false;
	if (!ActiveSnapshotSet())
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		*pushed_snapshot = true;
	}

	qdesc = CreateQueryDesc(pstmt, sourceText, GetActiveSnapshot(),
							InvalidSnapshot, dest, NULL, NULL, 0);
	ExecutorStart(qdesc, eflags);
	return qdesc;
}

/*
 * Execute a plan built by LrBuildApplyPlan() and return the report rows
 * (LrReportTupleDesc() layout) in a tuplestore allocated in resultcxt.
 *
 * An APPLY plan writes on the segments, so the transaction is marked as
 * writing before dispatch; that is what makes the QD hand the writer gang a
 * two-phase transaction context.
 */
Tuplestorestate *
LrExecuteApplyPlan(PlannedStmt *pstmt, const LrPlanSpec *spec,
				   const char *sourceText, MemoryContext resultcxt)
{
	MemoryContext oldcxt;
	Tuplestorestate *tstore;
	DestReceiver *dest;
	QueryDesc  *qdesc;
	bool		pushed;

	oldcxt = MemoryContextSwitchTo(resultcxt);
	tstore = tuplestore_begin_heap(true, false, work_mem);
	MemoryContextSwitchTo(oldcxt);

	dest = CreateTuplestoreDestReceiver();
	SetTuplestoreDestReceiverParams(dest, tstore, resultcxt, false,
									LrReportTupleDesc(),
									"LogicalApply report does not match the expected row type");

	if (spec->mode == LR_PLAN_APPLY)
		ExecutorMarkTransactionDoesWrites();

	qdesc = start_plan(pstmt, sourceText, dest, 0, &pushed);
	ExecutorRun(qdesc, ForwardScanDirection, 0, true);
	ExecutorFinish(qdesc);
	ExecutorEnd(qdesc);
	FreeQueryDesc(qdesc);
	dest->rDestroy(dest);
	if (pushed)
		PopActiveSnapshot();

	return tstore;
}

/*
 * EXPLAIN text of a plan built by LrBuildApplyPlan(), without running it.
 */
char *
LrExplainApplyPlan(PlannedStmt *pstmt, const char *sourceText)
{
	ExplainState *es = NewExplainState();
	QueryDesc  *qdesc;
	bool		pushed;

	es->verbose = true;
	es->format = EXPLAIN_FORMAT_TEXT;

	qdesc = start_plan(pstmt, sourceText, None_Receiver,
					   EXEC_FLAG_EXPLAIN_ONLY, &pushed);
	ExplainBeginOutput(es);
	ExplainPrintPlan(es, qdesc);
	ExplainEndOutput(es);
	ExecutorEnd(qdesc);
	FreeQueryDesc(qdesc);
	if (pushed)
		PopActiveSnapshot();

	return es->str->data;
}


/* ------------------------------------------------------------------------
 * Relation map bookkeeping shared by the operators (see mpp_apply.h)
 * ------------------------------------------------------------------------
 */

static List *lr_open_relmap_entries = NIL;
static bool lr_relmap_callback_registered = false;

static void
lr_relmap_xact_callback(XactEvent event, void *arg)
{
	ListCell   *lc;

	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/* relcache refs and locks went with the resource owner */
			foreach(lc, lr_open_relmap_entries)
				((LogicalRepRelMapEntry *) lfirst(lc))->localrel = NULL;
			/* fall through */
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			list_free(lr_open_relmap_entries);
			lr_open_relmap_entries = NIL;
			break;
		default:
			break;
	}
}

void
LrRelmapTrackOpen(LogicalRepRelMapEntry *entry)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

	if (!lr_relmap_callback_registered)
	{
		RegisterXactCallback(lr_relmap_xact_callback, NULL);
		lr_relmap_callback_registered = true;
	}
	lr_open_relmap_entries = lappend(lr_open_relmap_entries, entry);
	MemoryContextSwitchTo(oldcxt);
}

void
LrRelmapTrackClose(LogicalRepRelMapEntry *entry)
{
	lr_open_relmap_entries = list_delete_ptr(lr_open_relmap_entries, entry);
}
