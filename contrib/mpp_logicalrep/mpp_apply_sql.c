/*-------------------------------------------------------------------------
 *
 * mpp_apply_sql.c
 *	  SQL entry points of MPP logical replication apply.
 *
 * The apply worker drives a normal coordinator session through these
 * functions; each one builds the two-slice plan of mpp_apply_plan.c for one
 * mode, dispatches it and returns the report rows:
 *
 *   gp_lr_connect(subid, token, spec)          CONNECT
 *   gp_lr_drain(subid, token, gxid, parts, wait_ms, release_gxid,
 *               release_local)                 DRAIN
 *   gp_lr_apply(subid, token, txns)            APPLY (inside BEGIN/LOCK/COMMIT)
 *   gp_lr_explain(subid, token, mode, spec)    EXPLAIN of the plan
 *
 * The payload formats are those of mpp_apply.h; the functions only assemble
 * them from SQL arrays.  gp_lr_progress(), which records the batch in the
 * progress tables, is a SQL-language function (system_views.sql).
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_apply_sql.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/pg_type.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdistributedsnapshot.h"
#include "commands/copy.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_node.h"
#include "mpp_apply.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

static Datum run_plan(FunctionCallInfo fcinfo, LrPlanMode mode, Oid subid,
					  int64 token, const char *payload, const char *fname);
static void append_int4_list(StringInfo buf, const char *key, ArrayType *arr);
static void append_int8_list(StringInfo buf, const char *key, ArrayType *arr);
static void append_text_list(StringInfo buf, const char *key, ArrayType *arr);
static void append_oid_list(StringInfo buf, const char *key, ArrayType *arr);

static void
check_privilege(void)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drive MPP logical replication apply")));
}

/*
 * A copy-transport job name becomes part of a shell command (COPY FROM
 * PROGRAM) and of a filesystem path, so restrict it hard: 1..63 characters,
 * each of [A-Za-z0-9_.-], not starting with a dot or dash.
 */
static void
validate_job_name(const char *job)
{
	int			len = job ? (int) strlen(job) : 0;

	if (len < 1 || len > 63 || job[0] == '.' || job[0] == '-')
		goto bad;
	for (int i = 0; i < len; i++)
	{
		char		c = job[i];

		if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			  (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '-'))
			goto bad;
	}
	return;
bad:
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid copy-transport job name"),
			 errdetail("Job names must be 1-63 characters of [A-Za-z0-9_.-] and not begin with '.' or '-'.")));
}

/*
 * gp_lr_connect(subid oid, token int8, spec text)
 */
PG_FUNCTION_INFO_V1(gp_lr_connect);
Datum
gp_lr_connect(PG_FUNCTION_ARGS)
{
	Oid			subid = PG_GETARG_OID(0);
	int64		token = PG_GETARG_INT64(1);
	char	   *spec = text_to_cstring(PG_GETARG_TEXT_PP(2));

	check_privilege();
	return run_plan(fcinfo, LR_PLAN_CONNECT, subid, token, spec, "connect");
}

/*
 * gp_lr_drain(subid oid, token int8, gxid int8, parts int4[], wait_ms int4,
 *             release_gxid int8[], release_local text[])
 *
 * gxid 0 is the long poll.  parts NULL means every segment takes part.
 * release_local entries are "<seg>:<lsn>".
 */
PG_FUNCTION_INFO_V1(gp_lr_drain);
Datum
gp_lr_drain(PG_FUNCTION_ARGS)
{
	Oid			subid;
	int64		token;
	StringInfoData buf;

	check_privilege();
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("subid and token must not be null")));
	subid = PG_GETARG_OID(0);
	token = PG_GETARG_INT64(1);

	initStringInfo(&buf);
	appendStringInfo(&buf, "gxid=" INT64_FORMAT "\n",
					 PG_ARGISNULL(2) ? (int64) 0 : PG_GETARG_INT64(2));
	if (!PG_ARGISNULL(3))
		append_int4_list(&buf, "parts", PG_GETARG_ARRAYTYPE_P(3));
	if (!PG_ARGISNULL(4))
		appendStringInfo(&buf, "wait_ms=%d\n", PG_GETARG_INT32(4));
	if (!PG_ARGISNULL(5))
		append_int8_list(&buf, "release_gxid", PG_GETARG_ARRAYTYPE_P(5));
	if (!PG_ARGISNULL(6))
		append_text_list(&buf, "release_local", PG_GETARG_ARRAYTYPE_P(6));

	return run_plan(fcinfo, LR_PLAN_DRAIN, subid, token, buf.data, "drain");
}

/*
 * gp_lr_apply(subid oid, token int8, txns text[], defer oid[], catchup oid[])
 *
 * txns entries are "<seg>:<gxid>" or "<seg>:L<lsn>", in apply order.  defer
 * and catchup are remote relids: the tables whose initial copy is still
 * running are skipped, or, in a catch-up plan, the only ones emitted.
 */
PG_FUNCTION_INFO_V1(gp_lr_apply);
Datum
gp_lr_apply(PG_FUNCTION_ARGS)
{
	Oid			subid;
	int64		token;
	StringInfoData buf;

	check_privilege();
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("subid, token and txns must not be null")));
	subid = PG_GETARG_OID(0);
	token = PG_GETARG_INT64(1);

	initStringInfo(&buf);
	append_text_list(&buf, "txns", PG_GETARG_ARRAYTYPE_P(2));
	if (!PG_ARGISNULL(3))
		append_oid_list(&buf, "defer", PG_GETARG_ARRAYTYPE_P(3));
	if (!PG_ARGISNULL(4))
		append_oid_list(&buf, "catchup", PG_GETARG_ARRAYTYPE_P(4));
	return run_plan(fcinfo, LR_PLAN_APPLY, subid, token, buf.data, "apply");
}

/*
 * gp_lr_explain(subid oid, token int8, mode int4, spec text) -> text
 */
PG_FUNCTION_INFO_V1(gp_lr_explain);
Datum
gp_lr_explain(PG_FUNCTION_ARGS)
{
	LrPlanSpec	spec;
	PlannedStmt *pstmt;

	check_privilege();
	memset(&spec, 0, sizeof(spec));
	spec.subid = PG_GETARG_OID(0);
	spec.token = PG_GETARG_INT64(1);
	spec.mode = (LrPlanMode) PG_GETARG_INT32(2);
	spec.nsegs = getgpsegmentCount();
	spec.payload = PG_ARGISNULL(3) ? NULL : text_to_cstring(PG_GETARG_TEXT_PP(3));
	if (spec.mode < LR_PLAN_CONNECT || spec.mode > LR_PLAN_APPLY)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("mode must be 0 (CONNECT), 1 (DRAIN) or 2 (APPLY)")));

	pstmt = LrBuildApplyPlan(&spec);
	PG_RETURN_TEXT_P(cstring_to_text(LrExplainApplyPlan(pstmt, "gp_lr_explain()")));
}

/* ------------------------------------------------------------------------ */

static Datum
run_plan(FunctionCallInfo fcinfo, LrPlanMode mode, Oid subid, int64 token,
		 const char *payload, const char *fname)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LrPlanSpec	spec;
	PlannedStmt *pstmt;
	TupleDesc	tupdesc;

	if (!rsinfo || !IsA(rsinfo, ReturnSetInfo) ||
		!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	memset(&spec, 0, sizeof(spec));
	spec.mode = mode;
	spec.subid = subid;
	spec.token = token;
	spec.nsegs = getgpsegmentCount();
	spec.payload = payload;

	pstmt = LrBuildApplyPlan(&spec);

	/*
	 * This is what a segment reports as the query of the gang processes, and
	 * those gangs are resident: they sit idle between drains for as long as
	 * the subscription runs, looking like the QEs of an ordinary session that
	 * has gone quiet.  Say whose they are, so that whoever finds them on a
	 * segment can tell what they are before deciding what to do about them.
	 */
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = LrExecuteApplyPlan(pstmt, &spec,
										   psprintf("mpp_logicalrep %s, subscription %u",
													fname, subid),
										   rsinfo->econtext->ecxt_per_query_memory);
	rsinfo->setDesc = tupdesc;
	PG_RETURN_NULL();
}

static void
deconstruct(ArrayType *arr, Oid elmtype, int elmlen, bool elmbyval, char elmalign,
			const char *key, Datum **elems, int *n)
{
	bool	   *nulls;

	if (ARR_ELEMTYPE(arr) != elmtype)
		elog(ERROR, "unexpected element type for %s", key);
	deconstruct_array(arr, elmtype, elmlen, elmbyval, elmalign, elems, &nulls, n);
	for (int i = 0; i < *n; i++)
	{
		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("%s must not contain nulls", key)));
	}
}

static void
append_int4_list(StringInfo buf, const char *key, ArrayType *arr)
{
	Datum	   *elems;
	int			n;

	deconstruct(arr, INT4OID, sizeof(int32), true, TYPALIGN_INT, key, &elems, &n);
	appendStringInfo(buf, "%s=", key);
	for (int i = 0; i < n; i++)
		appendStringInfo(buf, "%s%d", i ? "," : "", DatumGetInt32(elems[i]));
	appendStringInfoChar(buf, '\n');
}

static void
append_int8_list(StringInfo buf, const char *key, ArrayType *arr)
{
	Datum	   *elems;
	int			n;

	deconstruct(arr, INT8OID, sizeof(int64), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE, key,
				&elems, &n);
	appendStringInfo(buf, "%s=", key);
	for (int i = 0; i < n; i++)
		appendStringInfo(buf, "%s" INT64_FORMAT, i ? "," : "", DatumGetInt64(elems[i]));
	appendStringInfoChar(buf, '\n');
}

static void
append_oid_list(StringInfo buf, const char *key, ArrayType *arr)
{
	Datum	   *elems;
	int			n;

	deconstruct(arr, OIDOID, sizeof(Oid), true, TYPALIGN_INT, key, &elems, &n);
	appendStringInfo(buf, "%s=", key);
	for (int i = 0; i < n; i++)
		appendStringInfo(buf, "%s%u", i ? "," : "", DatumGetObjectId(elems[i]));
	appendStringInfoChar(buf, '\n');
}

static void
append_text_list(StringInfo buf, const char *key, ArrayType *arr)
{
	Datum	   *elems;
	int			n;

	deconstruct(arr, TEXTOID, -1, false, TYPALIGN_INT, key, &elems, &n);
	appendStringInfo(buf, "%s=", key);
	for (int i = 0; i < n; i++)
	{
		char	   *s = TextDatumGetCString(elems[i]);

		if (strchr(s, ',') || strchr(s, '\n'))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s entry \"%s\" must not contain ',' or newline", key, s)));
		appendStringInfo(buf, "%s%s", i ? "," : "", s);
	}
	appendStringInfoChar(buf, '\n');
}


/* ------------------------------------------------------------------------
 * Initial-sync bulk transport
 *
 * gp_lr_copy_recv() and gp_lr_copy_ports() are the receive-side SQL surface
 * of the segment-to-segment COPY transport.  The C helper cbdb_lr_copyhelper
 * only moves framed bytes (seg i -> target i); all hash redistribution is
 * done by the planner's Motion under the driving
 *
 *     INSERT INTO t SELECT * FROM gp_lr_copy_recv('t'::regclass, job, n) AS t(...)
 *
 * so the transport itself is table-agnostic.  Both functions run EXECUTE ON
 * ALL SEGMENTS: each QE spawns/serves its own helper against its own $PGDATA.
 * ------------------------------------------------------------------------
 */

/* GUCs shared with the copy transport (guc_gp.c) */
int			gp_lr_copy_port_lo = 42000;
int			gp_lr_copy_port_hi = 42999;
bool		gp_lr_copy_crc = true;

/*
 * Which address the copy transport listens on.  Empty means every interface,
 * which is what a single-homed host wants and what a host with a public
 * interface does not: the socket writes what it is given into the table being
 * copied, and its only authentication is that the sender opens with the job
 * name (a secret, see mpp_tablesync.c).  On a multi-homed host name the
 * address the source cluster reaches this segment on.
 */
char	   *gp_lr_copy_bind_address = "";

typedef struct LrCopyRecvState
{
	Relation	rel;
	CopyFromState cstate;
	EState	   *estate;
	ExprContext *econtext;
	Datum	   *values;
	bool	   *nulls;
} LrCopyRecvState;

/*
 * gp_lr_copy_recv(rel regclass, job text, nsources int4) RETURNS SETOF record
 *
 * Runs on every segment.  Spawns "cbdb_lr_copyhelper recv" as a COPY FROM
 * PROGRAM data source, publishing this segment's listen port to
 * $PGDATA/gp_lr/<job>.<seg>.port, waits for its share of the <nsources>
 * source segments to connect, and yields the received rows (parsed with
 * rel's tuple descriptor).  The caller wraps it in INSERT ... SELECT so that
 * the rows are redistributed to their correct target segments by the plan's
 * Motion.
 *
 * N:M mapping.  Source segment S sends to target (S mod M), M being the
 * number of target segments (the helper's "send" side applies the same
 * rule to its --targets list, which the driver orders by target content
 * id).  So target j expects nsources/M connections, plus one if
 * j < nsources mod M.  A target that expects none returns no rows at once
 * and publishes no port file, so the driver must wait for exactly
 * min(nsources, M) rows from gp_lr_copy_ports() before starting the send.
 *
 * The result rows are shaped by the mandatory column-definition list, which
 * the driver generates to match rel; rel supplies the COPY input functions.
 */
PG_FUNCTION_INFO_V1(gp_lr_copy_recv);
Datum
gp_lr_copy_recv(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	LrCopyRecvState *st;
	MemoryContext oldcxt;

	if (SRF_IS_FIRSTCALL())
	{
		Oid			relid = PG_GETARG_OID(0);
		text	   *job = PG_GETARG_TEXT_PP(1);
		int32		nsources = PG_GETARG_INT32(2);
		int32		ntargets = getgpsegmentCount();
		int32		nclients;
		char	   *jobstr;
		StringInfoData cmd;
		TupleDesc	tupdesc;
		List	   *copy_opts;

		check_privilege();

		funcctx = SRF_FIRSTCALL_INIT();
		oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("gp_lr_copy_recv() requires a column definition list")));
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		st = (LrCopyRecvState *) palloc0(sizeof(LrCopyRecvState));

		/* AccessShareLock: we only parse into rel's descriptor, never write */
		st->rel = table_open(relid, AccessShareLock);

		jobstr = text_to_cstring(job);
		validate_job_name(jobstr);

		if (nsources < 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("nsources must not be negative")));
		if (GpIdentity.segindex < 0 || ntargets <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("gp_lr_copy_recv() must run on a segment")));
		nclients = nsources / ntargets +
			((GpIdentity.segindex < nsources % ntargets) ? 1 : 0);

		initStringInfo(&cmd);
		/*
		 * <SEGID> is substituted per segment by MangleCopyFileName (which
		 * also requires the token to be present when on_segment is set).
		 */
		appendStringInfo(&cmd,
						 "cbdb_lr_copyhelper recv --job %s --seg <SEGID> --clients %d --port-range %d-%d",
						 jobstr, nclients,
						 gp_lr_copy_port_lo, gp_lr_copy_port_hi);
		if (gp_lr_copy_crc)
			appendStringInfoString(&cmd, " --crc");
		if (gp_lr_copy_bind_address[0] != '\0')
		{
			/* the command line goes to a shell: let nothing else through */
			for (const char *c = gp_lr_copy_bind_address; *c; c++)
				if (!((*c >= 'A' && *c <= 'Z') || (*c >= 'a' && *c <= 'z') ||
					  (*c >= '0' && *c <= '9') || *c == '.' || *c == ':' ||
					  *c == '-' || *c == '_'))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid mpp_logicalrep.copy_bind_address"),
							 errdetail("It must be a host address: letters, digits and \".:-_\".")));
			appendStringInfo(&cmd, " --bindaddr %s", gp_lr_copy_bind_address);
		}

		st->estate = CreateExecutorState();
		st->econtext = GetPerTupleExprContext(st->estate);
		st->values = (Datum *) palloc(sizeof(Datum) * funcctx->tuple_desc->natts);
		st->nulls = (bool *) palloc(sizeof(bool) * funcctx->tuple_desc->natts);

		/*
		 * on_segment is what makes a QE-side BeginCopyFrom read from the
		 * program locally (COPY_DIRECT).  Without it a QE assumes the data
		 * is streamed from the dispatcher (COPY_EXECUTOR) and blocks on the
		 * QD connection without ever spawning the helper.
		 */
		copy_opts = list_make1(makeDefElem("on_segment",
										   (Node *) makeInteger(true), -1));
		st->cstate = BeginCopyFrom(NULL, st->rel, NULL,
								   cmd.data, /* is_program */ true,
								   NULL, NULL, NIL, copy_opts);

		funcctx->user_fctx = st;
		MemoryContextSwitchTo(oldcxt);
	}

	funcctx = SRF_PERCALL_SETUP();
	st = (LrCopyRecvState *) funcctx->user_fctx;

	oldcxt = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
	ResetPerTupleExprContext(st->estate);

	if (NextCopyFrom(st->cstate, st->econtext, st->values, st->nulls))
	{
		HeapTuple	tuple = heap_form_tuple(funcctx->tuple_desc, st->values, st->nulls);

		MemoryContextSwitchTo(oldcxt);
		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}

	EndCopyFrom(st->cstate);
	FreeExecutorState(st->estate);
	table_close(st->rel, AccessShareLock);
	MemoryContextSwitchTo(oldcxt);
	SRF_RETURN_DONE(funcctx);
}

/*
 * gp_lr_copy_ports(job text) RETURNS TABLE(segid int4, port int4)
 *
 * Runs on every segment; reads $PGDATA/gp_lr/<job>.<seg>.port (written by a
 * running recv helper) and returns (this segment's content id, port).  The
 * coordinator session polls this to learn every target segment's listen port
 * before it starts the source-side "COPY (query) TO PROGRAM send".  A segment
 * whose helper has not yet published its port returns no row (poll again).
 */
PG_FUNCTION_INFO_V1(gp_lr_copy_ports);
Datum
gp_lr_copy_ports(PG_FUNCTION_ARGS)
{
	text	   *job = PG_GETARG_TEXT_PP(0);
	char	   *jobstr = text_to_cstring(job);
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcxt;
	char		path[MAXPGPATH];
	FILE	   *f;

	check_privilege();
	validate_job_name(jobstr);

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("gp_lr_copy_ports() must be called in a context that accepts a set")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcxt = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcxt);

	snprintf(path, sizeof(path), "%s/gp_lr/%s.%d.port",
			 DataDir, jobstr, GpIdentity.segindex);

	f = AllocateFile(path, "r");
	if (f != NULL)
	{
		int			port = 0;

		if (fscanf(f, "%d", &port) == 1 && port > 0)
		{
			Datum		values[2];
			bool		nulls[2] = {false, false};

			values[0] = Int32GetDatum(GpIdentity.segindex);
			values[1] = Int32GetDatum(port);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
		FreeFile(f);
	}

	return (Datum) 0;
}
