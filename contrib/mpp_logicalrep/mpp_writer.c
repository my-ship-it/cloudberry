/*-------------------------------------------------------------------------
 *
 * mpp_writer.c
 *	  LogicalApply: the writer-gang operator of MPP logical replication apply.
 *
 * One LogicalApply runs on every writer QE, inside the two-phase
 * transaction context the QD dispatched with the APPLY plan.  It consumes
 * the Explicit Motion channel (contract in mpp_apply.h): report
 * rows are passed through to the Gather Motion, data rows are applied to
 * the local relations, and at the end of an APPLY batch one summary report
 * per writer is emitted.
 *
 * The apply logic is the stock logical replication apply worker's, minus
 * the process model: relations are resolved by name through the relation
 * map of relation.c (fed by the R rows the readers forward), tuples are
 * decoded with proto.c and stored with worker.c's slot helpers, and the
 * changes are made with execReplication.c.  Unlike the apply worker, which
 * builds an executor state per change, a writer keeps one open relation
 * with its executor state for the duration of a batch, because a batch
 * carries many changes of few relations.
 *
 * Every change is one "replication step": it runs under a fresh snapshot
 * and is followed by CommandCounterIncrement(), so that later changes of
 * the same batch see earlier ones (the source stream may update a row it
 * inserted a moment ago).  AFTER triggers are queued and fired per change,
 * nested inside the enclosing plan's trigger query level.
 *
 * Triggers run with session_replication_role = replica while a batch is
 * applied, as in the apply worker; the setting is not synchronized from the
 * QD, so the writer sets it itself for the duration of the plan.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_writer.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_inherits.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "parser/parse_relation.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "mpp_apply.h"
#include "replication/worker_internal.h"
#include "storage/lmgr.h"
#include "utils/acl.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/*
 * One relation open for the current batch.  Keyed by the remote relation id
 * (the id used in the stream), which is what data rows carry.
 */
typedef struct LrApplyRel
{
	Oid			remoteid;		/* hash key */
	LogicalRepRelMapEntry *rel; /* open with RowExclusiveLock */
	EState	   *estate;
	ResultRelInfo *rri;			/* indexes open */
	TupleTableSlot *remoteslot; /* virtual, local tuple descriptor */
	TupleTableSlot *localslot;	/* table slot for the row found locally */
	bool		buffered_am;	/* AM keeps inserts in a buffer until dml_fini */
	bool		pending_inserts;	/* inserts since the last dml_init */
	bool		exclusive_locked;	/* ExclusiveLock taken for a non-block AM */
	int64		nrows;
	int64		nmissing;		/* UPDATE/DELETE whose row was not found */
} LrApplyRel;

typedef struct LrApplyState
{
	CustomScanState css;
	LrPlanSpec	spec;
	bool		active;			/* this process runs the writer slice */
	bool		child_done;
	bool		summary_sent;
	int64		nrows_in;		/* data rows received from the readers */
	int64		nbytes_in;

	/* apply machinery; only used on the active writer of an APPLY plan */
	MemoryContext batchcxt;		/* lives for one execution of the plan */
	MemoryContext rowcxt;		/* reset before every data row */
	HTAB	   *rels;			/* remoteid -> LrApplyRel */
	List	   *relorder;		/* LrApplyRel *, in open order */
	List	   *touched;		/* remote relids that received data changes */
	int			guc_nestlevel;	/* 0 when session_replication_role is not set */
	bool		snapshot_pushed;	/* the batch's snapshot is on the stack */
	bool		finished;		/* finish_batch() done */
} LrApplyState;

static Node *lr_apply_create_state(CustomScan *cscan);
static void lr_apply_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *lr_apply_exec(CustomScanState *node);
static void lr_apply_end(CustomScanState *node);
static void lr_apply_rescan(CustomScanState *node);
static void lr_apply_explain(CustomScanState *node, List *ancestors,
							 ExplainState *es);

static void begin_batch(LrApplyState *state);
static void finish_batch(LrApplyState *state);
static void apply_data_row(LrApplyState *state, char op,
						   const char *data, int len);
static LrApplyRel *open_apply_rel(LrApplyState *state, Oid remoteid);
static void close_apply_rel(LrApplyState *state, LrApplyRel *arel);
static void begin_step(LrApplyState *state, LrApplyRel *arel);
static void end_step(LrApplyRel *arel);
static bool find_local_tuple(LrApplyRel *arel);
static void handle_relation(LrApplyState *state, StringInfo s);
static void handle_insert(LrApplyState *state, StringInfo s);
static void handle_update(LrApplyState *state, StringInfo s);
static void handle_delete(LrApplyState *state, StringInfo s);
static void handle_truncate(LrApplyState *state, StringInfo s);
static void note_touched(LrApplyState *state, Oid remoteid);

const CustomScanMethods LrApplyScanMethods = {
	"LogicalApply",
	lr_apply_create_state
};

static const CustomExecMethods lr_apply_exec_methods = {
	.CustomName = "LogicalApply",
	.BeginCustomScan = lr_apply_begin,
	.ExecCustomScan = lr_apply_exec,
	.EndCustomScan = lr_apply_end,
	.ReScanCustomScan = lr_apply_rescan,
	.ExplainCustomScan = lr_apply_explain,
};

static Node *
lr_apply_create_state(CustomScan *cscan)
{
	LrApplyState *state = (LrApplyState *) newNode(sizeof(LrApplyState),
												   T_CustomScanState);

	state->css.methods = &lr_apply_exec_methods;
	LrSpecFromCustomScan(cscan, &state->spec);
	return (Node *) state;
}

static void
lr_apply_begin(CustomScanState *node, EState *estate, int eflags)
{
	LrApplyState *state = (LrApplyState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Plan	   *child;

	state->active = LrSliceIsLocal(estate);
	state->child_done = false;
	state->summary_sent = false;
	state->nrows_in = 0;
	state->nbytes_in = 0;
	state->batchcxt = NULL;
	state->rowcxt = NULL;
	state->rels = NULL;
	state->relorder = NIL;
	state->touched = NIL;
	state->guc_nestlevel = 0;
	state->finished = true;

	if (state->active)
		elog(DEBUG1, "LogicalApply %s: subscription %u, dtx context %s",
			 LrPlanModeName(state->spec.mode), state->spec.subid,
			 DtxContextToString(DistributedTransactionContext));

	/* The single child is the Explicit Redistribute Motion from the readers */
	if (list_length(cscan->custom_plans) != 1)
		elog(ERROR, "LogicalApply expects exactly one child plan");
	child = (Plan *) linitial(cscan->custom_plans);
	node->custom_ps = list_make1(ExecInitNode(child, estate, eflags));

	if (state->active && state->spec.mode == LR_PLAN_APPLY &&
		!(eflags & EXEC_FLAG_EXPLAIN_ONLY))
		begin_batch(state);
}

static TupleTableSlot *
lr_apply_next(ScanState *ss)
{
	LrApplyState *state = (LrApplyState *) ss;
	PlanState  *child = (PlanState *) linitial(state->css.custom_ps);

	if (!state->active)
		return NULL;

	while (!state->child_done)
	{
		TupleTableSlot *slot = ExecProcNode(child);
		char		op;
		bool		isnull;
		Datum		d;
		bytea	   *payload;

		if (TupIsNull(slot))
		{
			state->child_done = true;
			break;
		}

		d = slot_getattr(slot, LR_SRC_ATT_OP, &isnull);
		if (isnull)
			elog(ERROR, "LogicalApply received a row without op");
		op = DatumGetChar(d);

		d = slot_getattr(slot, LR_SRC_ATT_PAYLOAD, &isnull);
		if (isnull)
			elog(ERROR, "LogicalApply received a row without payload (op '%c')",
				 op);
		payload = DatumGetByteaPP(d);

		if (LrOpIsReport(op))
		{
			LrReport	rpt;

			LrReportDeserialize(VARDATA_ANY(payload), VARSIZE_ANY_EXHDR(payload),
								&rpt);
			return LrStoreReport(ss, &rpt);
		}

		/* Data row */
		state->nrows_in++;
		state->nbytes_in += VARSIZE_ANY_EXHDR(payload);

		if (state->spec.mode != LR_PLAN_APPLY)
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("LogicalApply received a data row (op '%c') in %s mode",
							op, LrPlanModeName(state->spec.mode))));

		apply_data_row(state, op, VARDATA_ANY(payload),
					   VARSIZE_ANY_EXHDR(payload));
	}

	/*
	 * Input exhausted.  Close the relations and revert the GUC now, while
	 * the enclosing plan is still executing: the enclosing executor's
	 * AfterTriggerEndQuery() runs before our EndCustomScan(), and the
	 * relations must be closed before the transaction prepares.
	 */
	if (!state->finished)
		finish_batch(state);

	/* An APPLY plan ends with one summary row per writer */
	if (state->spec.mode == LR_PLAN_APPLY && !state->summary_sent)
	{
		LrReport	rpt;

		state->summary_sent = true;
		memset(&rpt, 0, sizeof(rpt));
		rpt.seg = GpIdentity.segindex;
		rpt.kind = LR_REPORT_APPLIED;
		rpt.nrows = state->nrows_in;
		rpt.nbytes = state->nbytes_in;
		rpt.remote_relids = state->touched;
		return LrStoreReport(ss, &rpt);
	}

	return NULL;
}

static TupleTableSlot *
lr_apply_exec(CustomScanState *node)
{
	return ExecScan(&node->ss, lr_apply_next, LrScanRecheck);
}

static void
lr_apply_end(CustomScanState *node)
{
	LrApplyState *state = (LrApplyState *) node;
	ListCell   *lc;

	/* Normally done when the input ran dry; this covers an early shutdown. */
	if (!state->finished)
		finish_batch(state);

	foreach(lc, node->custom_ps)
		ExecEndNode((PlanState *) lfirst(lc));
}

static void
lr_apply_rescan(CustomScanState *node)
{
	LrApplyState *state = (LrApplyState *) node;
	ListCell   *lc;

	if (!state->finished)
		finish_batch(state);
	state->child_done = false;
	state->summary_sent = false;
	state->nrows_in = 0;
	state->nbytes_in = 0;
	foreach(lc, node->custom_ps)
		ExecReScan((PlanState *) lfirst(lc));
	if (state->active && state->spec.mode == LR_PLAN_APPLY)
		begin_batch(state);
}

static void
lr_apply_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	LrApplyState *state = (LrApplyState *) node;

	LrExplainSpec(&state->spec, es);
}

/* ------------------------------------------------------------------------
 * Batch lifetime
 * ------------------------------------------------------------------------
 */

static void
begin_batch(LrApplyState *state)
{
	EState	   *estate = state->css.ss.ps.state;
	HASHCTL		ctl;

	state->batchcxt = AllocSetContextCreate(estate->es_query_cxt,
											"LogicalApply batch",
											ALLOCSET_DEFAULT_SIZES);
	state->rowcxt = AllocSetContextCreate(state->batchcxt,
										  "LogicalApply row",
										  ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(LrApplyRel);
	ctl.hcxt = state->batchcxt;
	state->rels = hash_create("LogicalApply relations", 16, &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	state->relorder = NIL;
	state->touched = NIL;

	/*
	 * Apply with the replica role, like the apply worker does; reverted in
	 * finish_batch().  PGC_SUSET as the context lets a non-superuser
	 * subscription owner get there, as SetConfigOption() does for the
	 * worker.
	 */
	state->guc_nestlevel = NewGUCNestLevel();
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_SESSION);

	state->finished = false;
}

static void
finish_batch(LrApplyState *state)
{
	/* close_apply_rel() unlinks from relorder, so walk a copy */
	List	   *rels = list_copy(state->relorder);
	ListCell   *lc;

	foreach(lc, rels)
		close_apply_rel(state, (LrApplyRel *) lfirst(lc));
	list_free(rels);
	Assert(state->relorder == NIL);

	if (state->guc_nestlevel > 0)
	{
		AtEOXact_GUC(true, state->guc_nestlevel);
		state->guc_nestlevel = 0;
	}

	if (state->rels)
	{
		hash_destroy(state->rels);
		state->rels = NULL;
	}
	if (state->snapshot_pushed)
	{
		PopActiveSnapshot();
		state->snapshot_pushed = false;
	}
	/* batchcxt (with rowcxt) goes away with the executor's query context */
	state->finished = true;
}

/* ------------------------------------------------------------------------
 * Relations
 * ------------------------------------------------------------------------
 */

static LrApplyRel *
open_apply_rel(LrApplyState *state, Oid remoteid)
{
	LrApplyRel *arel;
	bool		found;
	MemoryContext oldcxt;
	EState	   *estate;
	RangeTblEntry *rte;
	List	   *perminfos = NIL;
	Relation	localrel;

	arel = (LrApplyRel *) hash_search(state->rels, &remoteid, HASH_ENTER,
									  &found);
	if (found)
		return arel;

	memset(arel, 0, sizeof(LrApplyRel));
	arel->remoteid = remoteid;

	oldcxt = MemoryContextSwitchTo(state->batchcxt);

	/* errors out if the readers did not send an R row for this relation */
	arel->rel = logicalrep_rel_open(remoteid, RowExclusiveLock);
	LrRelmapTrackOpen(arel->rel);
	localrel = arel->rel->localrel;

	if (localrel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("MPP logical replication cannot apply changes to relation \"%s.%s\" yet",
						arel->rel->remoterel.nspname,
						arel->rel->remoterel.relname),
				 errdetail("Only plain tables are supported; the target is a %s.",
						   localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE ?
						   "partitioned table" : "relation of another kind")));

	/*
	 * Executor state for this relation, as worker.c's
	 * create_edata_for_relation() builds it, but kept for the whole batch.
	 */
	arel->estate = estate = CreateExecutorState();

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(localrel);
	rte->relkind = localrel->rd_rel->relkind;
	rte->rellockmode = AccessShareLock;
	addRTEPermissionInfo(&perminfos, rte);
	ExecInitRangeTable(estate, list_make1(rte), perminfos);

	arel->rri = makeNode(ResultRelInfo);
	InitResultRelInfo(arel->rri, localrel, 1, NULL, 0);
	estate->es_opened_result_relations =
		lappend(estate->es_opened_result_relations, arel->rri);
	ExecOpenIndices(arel->rri, false);

	/*
	 * AO and other AMs need this before the first insert / delete.  Those
	 * AMs also keep inserted rows in a buffer that only dml_fini writes out,
	 * so a lookup cannot see rows inserted earlier in the same batch until
	 * the buffer is flushed; see flush_pending_inserts().
	 */
	table_dml_init(localrel, CMD_INSERT);
	arel->buffered_am = (localrel->rd_tableam->dml_init != NULL);
	arel->pending_inserts = false;
	arel->exclusive_locked = false;

	arel->remoteslot = ExecInitExtraTupleSlot(estate, RelationGetDescr(localrel),
											  &TTSOpsVirtual);
	arel->localslot = table_slot_create(localrel, &estate->es_tupleTable);

	state->relorder = lappend(state->relorder, arel);

	MemoryContextSwitchTo(oldcxt);
	return arel;
}

static void
close_apply_rel(LrApplyState *state, LrApplyRel *arel)
{
	Relation	localrel = arel->rel->localrel;

	ExecCloseIndices(arel->rri);
	table_dml_fini(localrel, CMD_INSERT);
	ExecResetTupleTable(arel->estate->es_tupleTable, false);
	FreeExecutorState(arel->estate);

	/* the lock stays until the transaction ends */
	logicalrep_rel_close(arel->rel, NoLock);
	LrRelmapTrackClose(arel->rel);

	state->relorder = list_delete_ptr(state->relorder, arel);
	hash_search(state->rels, &arel->remoteid, HASH_REMOVE, NULL);
}

static void
note_touched(LrApplyState *state, Oid remoteid)
{
	if (!list_member_oid(state->touched, remoteid))
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(state->batchcxt);

		state->touched = lappend_oid(state->touched, remoteid);
		MemoryContextSwitchTo(oldcxt);
	}
}

/*
 * One change = one replication step (worker.c's begin/end_replication_step
 * plus the per-change AFTER trigger query level of create_edata/finish_edata).
 *
 * worker.c takes a fresh transaction snapshot for every change, because there
 * each change is a statement of its own in a read committed transaction.  A
 * batch here is one distributed transaction, and on a Cloudberry writer a
 * snapshot is not a cheap thing to take: GetSnapshotData() publishes it for
 * the segment's readers through a shared memory segment it attaches, writes
 * and detaches again -- an shm_open, an fstat, an mmap, a close and an munmap
 * per snapshot, which at one snapshot per row was over a third of the time
 * the apply spent.
 *
 * What a change actually needs of the snapshot is to see what the changes
 * before it in the same batch did, and that is the command id, not the
 * snapshot: end_step() advances the command counter and this advances the
 * snapshot's notion of it.  So the batch takes one snapshot, at its first
 * step, and finish_batch() drops it.
 *
 * What it gives up is seeing a local transaction that commits in the middle
 * of a batch.  A subscriber's tables are meant to be written by the apply and
 * nothing else, and a local writer that races it is already a conflict
 * whichever snapshot is used.
 */
static void
begin_step(LrApplyState *state, LrApplyRel *arel)
{
	EState	   *estate = arel->estate;

	if (!state->snapshot_pushed)
	{
		PushActiveSnapshot(GetTransactionSnapshot());
		state->snapshot_pushed = true;
	}
	else
		UpdateActiveSnapshotCommandId();
	ResetPerTupleExprContext(estate);
	estate->es_output_cid = GetCurrentCommandId(true);
	AfterTriggerBeginQuery();
}

static void
end_step(LrApplyRel *arel)
{
	AfterTriggerEndQuery(arel->estate);
	ExecClearTuple(arel->remoteslot);
	ExecClearTuple(arel->localslot);
	CommandCounterIncrement();
}

/*
 * Make rows inserted earlier in this batch visible to a lookup on an AM that
 * buffers inserts (AO, AOCO, PAX): close and re-open the DML state, which
 * writes the buffered block out.  Heap never gets here.
 */
static void
flush_pending_inserts(LrApplyRel *arel)
{
	Relation	localrel = arel->rel->localrel;

	if (!arel->buffered_am || !arel->pending_inserts)
		return;
	table_dml_fini(localrel, CMD_INSERT);

	/*
	 * dml_fini also wrote the AM's metadata (aoseg, visimap); the rest of
	 * this step must see it, so advance the command counter and let the
	 * batch's snapshot (pushed by begin_step) see that far.
	 */
	CommandCounterIncrement();
	UpdateActiveSnapshotCommandId();
	arel->estate->es_output_cid = GetCurrentCommandId(true);

	table_dml_init(localrel, CMD_INSERT);
	arel->pending_inserts = false;
}

/*
 * Locate the local row matching the replica identity in remoteslot; on
 * success it is locked and stored in localslot.
 *
 * AO, AOCO and PAX have no row locks: UPDATE and DELETE on them serialize
 * through ExclusiveLock on the relation, as Cloudberry's own DML does, and
 * RelationFindReplTuple* skips the row lock for them.  Taken lazily so that
 * an INSERT-only batch keeps the ordinary RowExclusiveLock.
 */
static bool
find_local_tuple(LrApplyRel *arel)
{
	LogicalRepRelMapEntry *rel = arel->rel;
	Relation	localrel = rel->localrel;

	TargetPrivilegesCheck(localrel, ACL_SELECT);
	if (RelationIsNonblockRelation(localrel) && !arel->exclusive_locked)
	{
		LockRelationOid(RelationGetRelid(localrel), ExclusiveLock);
		arel->exclusive_locked = true;
	}
	flush_pending_inserts(arel);

	if (OidIsValid(rel->localindexoid))
		return RelationFindReplTupleByIndex(localrel, rel->localindexoid,
											LockTupleExclusive,
											arel->remoteslot, arel->localslot);

	Assert(rel->remoterel.replident == REPLICA_IDENTITY_FULL);
	return RelationFindReplTupleSeq(localrel, LockTupleExclusive,
									arel->remoteslot, arel->localslot);
}

/* ------------------------------------------------------------------------
 * Data rows
 * ------------------------------------------------------------------------
 */

static void
apply_data_row(LrApplyState *state, char op, const char *data, int len)
{
	StringInfoData s;
	MemoryContext oldcxt;

	MemoryContextReset(state->rowcxt);
	oldcxt = MemoryContextSwitchTo(state->rowcxt);

	/*
	 * A private, NUL-terminated copy: pq_getmsgstring() relies on the
	 * terminator, and the bytea may point into a tuple we do not own.
	 */
	s.data = palloc(len + 1);
	memcpy(s.data, data, len);
	s.data[len] = '\0';
	s.len = len;
	s.maxlen = len + 1;
	s.cursor = 0;

	switch (op)
	{
		case LR_OP_RELATION:
			handle_relation(state, &s);
			break;
		case LR_OP_TYPE:
			{
				LogicalRepTyp typ;

				/* as apply_handle_type(): nothing to do with it */
				logicalrep_read_typ(&s, &typ);
			}
			break;
		case LR_OP_BEGIN:
		case LR_OP_END:
			/* transaction boundaries are bookkeeping for the readers */
			break;
		case LR_OP_INSERT:
			handle_insert(state, &s);
			break;
		case LR_OP_UPDATE:
			handle_update(state, &s);
			break;
		case LR_OP_DELETE:
			handle_delete(state, &s);
			break;
		case LR_OP_TRUNCATE:
			handle_truncate(state, &s);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("LogicalApply received a row with unknown op '%c'",
							op)));
	}

	MemoryContextSwitchTo(oldcxt);
}

/* same remote relation description? */
static bool
remoterel_equal(const LogicalRepRelation *a, const LogicalRepRelation *b)
{
	if (a->remoteid != b->remoteid || a->natts != b->natts ||
		a->replident != b->replident ||
		strcmp(a->nspname, b->nspname) != 0 ||
		strcmp(a->relname, b->relname) != 0 ||
		!bms_equal(a->attkeys, b->attkeys))
		return false;
	for (int i = 0; i < a->natts; i++)
	{
		if (a->atttyps[i] != b->atttyps[i] ||
			strcmp(a->attnames[i], b->attnames[i]) != 0)
			return false;
	}
	return true;
}

static void
handle_relation(LrApplyState *state, StringInfo s)
{
	LogicalRepRelation *remoterel = logicalrep_read_rel(s);
	LrApplyRel *arel;

	/*
	 * The readers describe every relation once per transaction (and
	 * pgoutput repeats descriptions inside streamed transactions), so most R
	 * rows repeat what the open relation already has: nothing to do then.
	 * A changed description invalidates the map entry; if the relation is
	 * open in this batch, close it first so that the next change re-opens
	 * it with the new description.
	 */
	arel = (LrApplyRel *) hash_search(state->rels, &remoterel->remoteid,
									  HASH_FIND, NULL);
	if (arel)
	{
		if (remoterel_equal(&arel->rel->remoterel, remoterel))
			return;
		close_apply_rel(state, arel);
	}

	logicalrep_relmap_update(remoterel);
}

static void
handle_insert(LrApplyState *state, StringInfo s)
{
	LogicalRepTupleData newtup;
	LogicalRepRelId remoteid;
	LrApplyRel *arel;
	MemoryContext oldcxt;

	remoteid = logicalrep_read_insert(s, &newtup);
	arel = open_apply_rel(state, remoteid);

	begin_step(state, arel);

	oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(arel->estate));
	slot_store_data(arel->remoteslot, arel->rel, &newtup);
	slot_fill_defaults(arel->rel, arel->estate, arel->remoteslot);
	MemoryContextSwitchTo(oldcxt);

	TargetPrivilegesCheck(arel->rel->localrel, ACL_INSERT);
	ExecSimpleRelationInsert(arel->rri, arel->estate, arel->remoteslot);
	arel->pending_inserts = true;

	end_step(arel);
	arel->nrows++;
	note_touched(state, remoteid);
}

static void
handle_update(LrApplyState *state, StringInfo s)
{
	LogicalRepTupleData oldtup;
	LogicalRepTupleData newtup;
	bool		has_oldtup;
	LogicalRepRelId remoteid;
	LrApplyRel *arel;
	EPQState	epqstate;
	MemoryContext oldcxt;
	bool		found;

	remoteid = logicalrep_read_update(s, &has_oldtup, &oldtup, &newtup);
	arel = open_apply_rel(state, remoteid);
	check_relation_updatable(arel->rel);

	begin_step(state, arel);

	/* the key to search by: the old tuple when sent, else the new one */
	oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(arel->estate));
	slot_store_data(arel->remoteslot, arel->rel,
					has_oldtup ? &oldtup : &newtup);
	MemoryContextSwitchTo(oldcxt);

	EvalPlanQualInit(&epqstate, arel->estate, NULL, NIL, -1, NIL);

	found = find_local_tuple(arel);
	ExecClearTuple(arel->remoteslot);

	if (found)
	{
		oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(arel->estate));
		slot_modify_data(arel->remoteslot, arel->localslot, arel->rel, &newtup);
		MemoryContextSwitchTo(oldcxt);

		EvalPlanQualSetSlot(&epqstate, arel->remoteslot);

		TargetPrivilegesCheck(arel->rel->localrel, ACL_UPDATE);
		ExecSimpleRelationUpdate(arel->rri, arel->estate, &epqstate,
								 arel->localslot, arel->remoteslot);
		arel->pending_inserts = true;
	}
	else
	{
		/*
		 * The apply worker logs this at DEBUG1; a missing row is an anomaly
		 * for an MPP replica, so keep it visible until the gp_lr_missing_row
		 * policy exists.
		 */
		arel->nmissing++;
		ereport(LOG,
				(errmsg("logical replication did not find row to be updated "
						"in replication target relation \"%s\"",
						RelationGetRelationName(arel->rel->localrel))));
	}

	EvalPlanQualEnd(&epqstate);

	end_step(arel);
	arel->nrows++;
	note_touched(state, remoteid);
}

static void
handle_delete(LrApplyState *state, StringInfo s)
{
	LogicalRepTupleData oldtup;
	LogicalRepRelId remoteid;
	LrApplyRel *arel;
	EPQState	epqstate;
	MemoryContext oldcxt;
	bool		found;

	remoteid = logicalrep_read_delete(s, &oldtup);
	arel = open_apply_rel(state, remoteid);
	check_relation_updatable(arel->rel);

	begin_step(state, arel);

	oldcxt = MemoryContextSwitchTo(GetPerTupleMemoryContext(arel->estate));
	slot_store_data(arel->remoteslot, arel->rel, &oldtup);
	MemoryContextSwitchTo(oldcxt);

	EvalPlanQualInit(&epqstate, arel->estate, NULL, NIL, -1, NIL);

	found = find_local_tuple(arel);

	if (found)
	{
		EvalPlanQualSetSlot(&epqstate, arel->localslot);

		TargetPrivilegesCheck(arel->rel->localrel, ACL_DELETE);
		ExecSimpleRelationDelete(arel->rri, arel->estate, &epqstate,
								 arel->localslot);
	}
	else
	{
		/*
		 * The apply worker logs this at DEBUG1; a missing row is an anomaly
		 * for an MPP replica, so keep it visible until the gp_lr_missing_row
		 * policy exists.
		 */
		arel->nmissing++;
		ereport(LOG,
				(errmsg("logical replication did not find row to be deleted "
						"in replication target relation \"%s\"",
						RelationGetRelationName(arel->rel->localrel))));
	}

	EvalPlanQualEnd(&epqstate);

	end_step(arel);
	arel->nrows++;
	note_touched(state, remoteid);
}

/*
 * TRUNCATE, as apply_handle_truncate().  The relations are re-opened with
 * AccessExclusiveLock; the QD already holds that lock on its side, having
 * seen the T in the DRAIN report.
 */
static void
handle_truncate(LrApplyState *state, StringInfo s)
{
	bool		cascade = false;
	bool		restart_seqs = false;
	List	   *remote_relids;
	List	   *remote_rels = NIL;
	List	   *rels = NIL;
	List	   *part_rels = NIL;
	List	   *relids = NIL;
	List	   *relids_logged = NIL;
	ListCell   *lc;
	LOCKMODE	lockmode = AccessExclusiveLock;

	remote_relids = logicalrep_read_truncate(s, &cascade, &restart_seqs);

	PushActiveSnapshot(GetTransactionSnapshot());

	foreach(lc, remote_relids)
	{
		LogicalRepRelId remoteid = lfirst_oid(lc);
		LrApplyRel *arel;
		LogicalRepRelMapEntry *rel;

		/* release our batch-scoped open of the relation first */
		arel = (LrApplyRel *) hash_search(state->rels, &remoteid, HASH_FIND,
										  NULL);
		if (arel)
			close_apply_rel(state, arel);

		rel = logicalrep_rel_open(remoteid, lockmode);
		remote_rels = lappend(remote_rels, rel);
		TargetPrivilegesCheck(rel->localrel, ACL_TRUNCATE);
		rels = lappend(rels, rel->localrel);
		relids = lappend_oid(relids, rel->localreloid);
		if (RelationIsLogicallyLogged(rel->localrel))
			relids_logged = lappend_oid(relids_logged, rel->localreloid);

		if (rel->localrel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
		{
			ListCell   *child;
			List	   *children = find_all_inheritors(rel->localreloid,
													   lockmode, NULL);

			foreach(child, children)
			{
				Oid			childrelid = lfirst_oid(child);
				Relation	childrel;

				if (list_member_oid(relids, childrelid))
					continue;

				/* find_all_inheritors already got lock */
				childrel = table_open(childrelid, NoLock);

				if (RELATION_IS_OTHER_TEMP(childrel))
				{
					table_close(childrel, lockmode);
					continue;
				}

				TargetPrivilegesCheck(childrel, ACL_TRUNCATE);
				rels = lappend(rels, childrel);
				part_rels = lappend(part_rels, childrel);
				relids = lappend_oid(relids, childrelid);
				if (RelationIsLogicallyLogged(childrel))
					relids_logged = lappend_oid(relids_logged, childrelid);
			}
		}

		note_touched(state, remoteid);
	}

	/*
	 * No cascading beyond what the stream names, and no owner switch: the
	 * writer already runs as the driving session's user.
	 */
	ExecuteTruncateGuts(rels, relids, relids_logged, DROP_RESTRICT,
						restart_seqs, false, NULL);

	foreach(lc, remote_rels)
		logicalrep_rel_close((LogicalRepRelMapEntry *) lfirst(lc), NoLock);
	foreach(lc, part_rels)
		table_close((Relation) lfirst(lc), NoLock);

	PopActiveSnapshot();
	CommandCounterIncrement();
}
