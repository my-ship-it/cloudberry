/*-------------------------------------------------------------------------
 *
 * mpp_apply_worker.c
 *	  The apply driver of MPP logical replication (see mpp_apply_worker.h).
 *
 * Main loop:
 *
 *   load progress            gp_subscription_progress / _applied of the target
 *   connect readers          gp_lr_connect(spec built from progress)
 *   connect source stream    walreceiver to the coordinator slot, cbdb_events
 *   loop
 *     event dcommit(G, segs) -> gp_lr_drain(G, segs, release of last batch)
 *                               ready rows -> registry; G -> batch
 *     idle                   -> gp_lr_drain(0) now and then, for local txns
 *     batch full / old       -> BEGIN; LOCK …; gp_lr_apply; gp_lr_progress;
 *                               COMMIT; feedback to the coordinator slot
 *     any error / cold       -> ROLLBACK, reconnect readers from progress,
 *                               restart the stream from the last committed
 *                               coordinator position (RECOVER)
 *
 * Replay after RECOVER or a restart is safe: readers drop transactions the
 * progress table lists as applied and report 'D' for an event whose gxid is
 * already applied, so the driver skips it.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_apply_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"

#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_subscription.h"
#include "commands/dbcommands.h"
#include "libpq/pqsignal.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "postmaster/postmaster.h"
#include "replication/worker_internal.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/syscache.h"
#include "libpq/pqformat.h"
#include "executor/tuptable.h"
#include "catalog/pg_type.h"
#include "storage/lockdefs.h"
#include "utils/tuplestore.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "pgstat.h"
#include "replication/logicalproto.h"
#include "logicalrep_sink.h"
#include "mpp_apply.h"
#include "mpp_apply_internal.h"
#include "mpp_apply_worker.h"
#include "replication/walreceiver.h"
#include "storage/latch.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

int			gp_lr_batch_max_txns = 64;
int			gp_lr_batch_max_bytes = 65536;
int			gp_lr_batch_max_latency_ms = 200;
int			gp_lr_event_wait_ms = 1000;
int			gp_lr_idle_drain_ms = 2000;
int			gp_lr_apply_error_action = LR_APPLY_ERROR_DISABLE;
int			gp_lr_apply_max_retries = 10;

#define LR_DCOMMIT_PREFIX		"cbdb.dcommit"
#define LR_FEEDBACK_MS			1000
#define LR_PENDING_CHECK_MS		2000	/* poll for tables added by REFRESH */
#define LR_MAX_SEGS				4096

/* one source segment's view of a transaction with a gxid */
typedef struct LrSegPart
{
	bool		ready;
	int64		seg_lsn;
	int64		nrows;
	int64		nbytes;
} LrSegPart;

/* a transaction with a gxid, as the readers reported it */
typedef struct LrGxidTxn
{
	int64		gxid;			/* hash key */
	LrSegPart  *parts;			/* per target segment */
	int			nready;
	List	   *rel_names;		/* char*, union over segments */
	int			lockmode;
	int64		nbytes;
	bool		has_event;
	int64		event_lsn;		/* coordinator position of the event */
	int			nsegs_expected; /* from the event; 0 = unknown */
	int16	   *segs_expected;
	bool		segs_exact;		/* the list is the set that wrote, so every
								 * one of them will report the transaction */
	bool		noop;			/* the event says no segment wrote anything */
	TimestampTz first_wait;		/* when we first waited for it */
	bool		in_batch;
	bool		warned;			/* "still incomplete" warning issued */
} LrGxidTxn;

/*
 * A transaction whose changes to a table that is still being copied were
 * held back.  Kept in commit order until the copy of
 * that table finishes and the driver catches it up; the reader keeps the
 * buffer because the driver does not release the transaction.
 *
 * A utility write is held the same way, and is told apart by gxid == 0: it
 * belongs to one segment, so seg and event_lsn (that segment's position) are
 * all of it and seg_lsn is not used.
 */
typedef struct LrDeferredTxn
{
	int64		gxid;			/* 0 = a local (utility) transaction */
	int64		xid;			/* local only: its xid on that segment */
	int			seg;			/* local only: the segment it belongs to */
	int64		event_lsn;		/* gxid: coordinator position; local: seg_lsn */
	int64	   *seg_lsn;		/* gxid only; per source segment, -1 = not there */
	int			lockmode;
} LrDeferredTxn;

/* remote relid -> "nsp.rel", as the readers report them */
typedef struct LrRelName
{
	Oid			relid;
	char	   *name;
} LrRelName;

/* a local (no gxid) transaction of one segment */
typedef struct LrLocalTxn
{
	int			seg;
	int64		xid;			/* its xid on that segment, from the report */
	int64		seg_lsn;
	List	   *rel_names;
	int			lockmode;
	int64		nbytes;
} LrLocalTxn;

/*
 * One source segment's local snapshot at a cut point, as
 * gp_lr_local_snapshot() froze it.  A local transaction visible under it
 * committed before the copy started reading, so the copy already contains
 * its rows.
 */
typedef struct LrLocalSnap
{
	TransactionId xmin;
	TransactionId xmax;			/* InvalidTransactionId = no snapshot */
	int			nxip;
	TransactionId *xip;
} LrLocalSnap;

typedef struct LrDriver
{
	const LrWorkerConfig *cfg;
	LrWorkerStats *stats;
	MemoryContext cxt;			/* whole run */
	MemoryContext batchcxt;		/* one batch */

	/* the target session, shared with the sink (mpp_apply_internal.h) */
	LrCbdbTransport *tr;
	SinkCaps	caps;			/* what the sink asked for */

	/* source */
	WalReceiverConn *src;
	List	   *seg_conninfos;	/* char*, by source content id */
	List	   *src_repl_relids;	/* Oid: DISTRIBUTED REPLICATED there */
	bool		src_has_secrets;	/* the source conninfo had a password, so
									 * the readers must add their own (O3) */
	int			nsrcsegs;
	int64		coord_pos;		/* last coordinator position received */
	int64		coord_committed;	/* coordinator position of the last batch */
	TimestampTz last_feedback;
	TimestampTz last_drain;

	/* progress (per target segment) */
	int64	   *seg_pos;
	int64	   *seg_flush;

	/* registry of ready / awaited transactions */
	HTAB	   *gxids;
	List	   *locals;			/* LrLocalTxn*, not yet in a batch */

	/* current batch */
	List	   *batch_gxids;	/* LrGxidTxn*, event order */
	List	   *batch_locals;	/* LrLocalTxn* */
	int64		batch_bytes;
	TimestampTz batch_started;

	/* what the next DRAIN releases */
	List	   *release_gxids;	/* int64* */
	List	   *release_locals; /* LrLocalTxn* */

	/* tables that may not receive changes yet, and their held transactions */
	List	   *defer_names;	/* char* "nsp.rel" (gp_subscription_rel) */
	List	   *relnames;		/* LrRelName* seen in reports */
	List	   *deferred_txns;	/* LrDeferredTxn*, event order */
	TimestampTz last_pending_check;

	/* the incremental synchronisation job, while one runs */
	LrWorkerConfig *mycfg;		/* the writable copy LrApplyWorkerRun made */
	struct LrSync *sync;
	List	   *sync_names;		/* its tables, "nsp.rel" */
	int64		sync_ds_xmax;
	int			sync_nds_inprogress;
	int64	   *sync_ds_inprogress;
	int			sync_nlocal;	/* the local half of the same cut point */
	LrLocalSnap *sync_local;
	bool		catchup_mode;	/* the copy is done: drain the held ones */
	int			stuck_errors;	/* errors in a row that committed nothing */

	/* the unit handed to the sink, rebuilt for each one */
	LogicalRepUnit unit;
	LrCbdbUnitSpec unit_spec;
	List	   *unit_batch;		/* LrDeferredTxn* of a catch-up unit */
	int			unit_dropped;	/* of those, ones the copy already had */
	int64		unit_coord_lsn;
	int64	   *unit_flush;		/* what unit_progress computed */

	TimestampTz started;

	/* what gp_stat_mpp_subscription reads, refreshed as the run goes (O2) */
	LrWorkerReport rep;
} LrDriver;

static void driver_load_source_segments(LrDriver *d);
static void driver_connect_readers(LrDriver *d);
static void driver_connect_source(LrDriver *d);
static void driver_disconnect_source(LrDriver *d);
static bool driver_pump_source(LrDriver *d, int timeout_ms);
static void driver_handle_event(LrDriver *d, int64 gxid, int64 lsn, int nsegs,
								const int16 *segs, bool exact);
static void driver_drain(LrDriver *d, int64 gxid, int nsegs, const int16 *segs,
						 int wait_ms);
static bool driver_batch_ready(LrDriver *d);
static void driver_send_feedback(LrDriver *d);
static bool unit_build_from_batch(LrDriver *d);
static bool unit_build_catchup(LrDriver *d);
static bool split_relname(const char *qualified, char **nspname, char **relname);
static LrGxidTxn *registry_get(LrDriver *d, int64 gxid, bool create);
static void registry_remove(LrDriver *d, LrGxidTxn *t);
static bool txn_complete(LrDriver *d, LrGxidTxn *t);
static bool gxid_covered_by_sync(LrDriver *d, int64 gxid);
static void driver_collect_ready(LrDriver *d);
static void driver_check_pending(LrDriver *d, bool force);
static bool driver_sync_progress(LrDriver *d);
static bool name_is_deferred(LrDriver *d, const char *name);
static bool txn_is_deferred(LrDriver *d, List *rel_names);
static void note_relnames(LrDriver *d, const char *relids, const char *names);
static char *relid_set(LrDriver *d, List *names);
static void driver_publish(LrDriver *d);
static void lr_disable_subscription(LrCbdbTransport *tr, Oid subid,
									const char *message);
static void run_initial_sync(LrWorkerConfig *cfg, LrCbdbTransport *tr);

/* ------------------------------------------------------------------------
 * Entry point
 *
 * Builds the two halves and hands them to the loop.  Everything below this
 * function is the Cloudberry assembly; the sink is in mpp_sink_cbdb.c and
 * the loop that drives them both is in logicalrep_apply_driver.c, which
 * knows about neither.
 * ------------------------------------------------------------------------
 */

void
LrApplyWorkerRun(const LrWorkerConfig *cfg, LrWorkerStats *stats)
{
	LrCbdbTransport *tr;
	LogicalRepSink *sink;
	LogicalRepAssembly *assembly;
	LrWorkerConfig *mycfg;
	MemoryContext oldcxt;
	MemoryContext cxt;

	cxt = AllocSetContextCreate(CurrentMemoryContext, "LrDriver",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);
	memset(stats, 0, sizeof(*stats));

	/* a private copy: the initial sync fills in the cut point */
	mycfg = palloc(sizeof(LrWorkerConfig));
	memcpy(mycfg, cfg, sizeof(LrWorkerConfig));

	tr = palloc0(sizeof(LrCbdbTransport));
	tr->conninfo = mycfg->target_conninfo;
	tr->subid = mycfg->subid;
	tr->token = mycfg->token;
	tr->stats = stats;

	/*
	 * Initial synchronisation runs first, over its own
	 * connections; only when gp_subscription_rel marks the subscription as
	 * ready does the streaming phase below begin.  Errors here end the run;
	 * whether the launcher then restarts the worker is decided the same way
	 * the streaming loop decides whether to replay.
	 */
	if (mycfg->initial_sync)
	{
		LrStatSetPhase(LR_PHASE_INITIAL_SYNC);
		run_initial_sync(mycfg, tr);
	}
	LrStatSetPhase(LR_PHASE_STREAMING);

	sink = LrMakeCbdbSink(tr);
	assembly = LrMakeCbdbAssembly(tr, mycfg, stats);

	lr_apply_driver_run(assembly, sink, NULL);

	LrStatSetPhase(LR_PHASE_STOPPING);
	MemoryContextSwitchTo(oldcxt);
	MemoryContextDelete(cxt);
}

/*
 * Disable the subscription in the extension's registry over the target
 * session, so that the launcher does not start the worker again.  (Its
 * pg_subscription row is disabled from the start -- that is what keeps the
 * server's own launcher off it -- so ALTER SUBSCRIPTION would say nothing
 * here.)  Failing to do so is not worth another error: the caller's message
 * is the one that matters.
 */
static void
lr_disable_subscription(LrCbdbTransport *tr, Oid subid, const char *message)
{
	MemoryContext cxt = CurrentMemoryContext;

	PG_TRY();
	{
		char	   *sql;

		if (tr->qd == NULL)
			LrTransportConnect(tr);
		sql = psprintf("UPDATE pg_catalog.gp_mpp_subscription "
					   "SET enabled = false, disable_reason = %s "
					   "WHERE subid = %u",
					   quote_literal_cstr(message), subid);
		PQclear(LrTransportExec(tr, sql));
		pfree(sql);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(cxt);
		FlushErrorState();
		elog(LOG, "mpp apply worker: could not disable subscription %u", subid);
	}
	PG_END_TRY();
}

/*
 * Errors of the initial synchronisation that no restart can get past, because
 * they are about what the two sides are rather than about what happened.
 *
 * This is a list of the certain ones, not the complement of the transient
 * ones the streaming loop uses: there the price of guessing wrong is one
 * replay out of gp_lr_apply_max_retries, here it is a subscription disabled
 * over what may have been a gang reset (which arrives as 58M01, in no
 * transient class).  Anything not listed keeps the old behaviour -- the
 * worker exits and the launcher starts it again, which redoes the sync.
 */
static bool
sync_error_is_permanent(int sqlerrcode)
{
	switch (sqlerrcode)
	{
		case ERRCODE_FEATURE_NOT_SUPPORTED: /* an AM whose changes are not decoded */
		case ERRCODE_UNDEFINED_TABLE:	/* a relation one side does not have */
		case ERRCODE_UNDEFINED_COLUMN:
		case ERRCODE_UNDEFINED_FUNCTION:	/* a source too old for what the
											 * synchronisation asks of it */
		case ERRCODE_INVALID_PARAMETER_VALUE:
		case ERRCODE_OBJECT_IN_USE:		/* the slots exist on some nodes only */
			return true;
	}
	return false;
}

/*
 * The initial synchronisation, with a verdict on its errors like the one the
 * streaming loop passes on its own.  Retrying is the
 * launcher's job here -- the worker exits and is started again -- and that is
 * right for a lost connection or a reset gang.  It is not right for something
 * the two sides do not agree on: a published table whose changes are not
 * decoded, a relation the target does not have.  Those come back on every
 * restart, and a worker restarted for ever is as quiet as no worker at all,
 * so the subscription stops and says why instead.
 */
static void
run_initial_sync(LrWorkerConfig *cfg, LrCbdbTransport *tr)
{
	MemoryContext cxt = CurrentMemoryContext;

	PG_TRY();
	{
		(void) LrInitialSync(cfg);
	}
	PG_CATCH();
	{
		ErrorData  *edata;
		int			sqlerrcode;
		char	   *message;

		MemoryContextSwitchTo(cxt);
		edata = CopyErrorData();
		FlushErrorState();
		sqlerrcode = edata->sqlerrcode;
		message = pstrdup(edata->message ? edata->message : "");

		if (!sync_error_is_permanent(sqlerrcode) ||
			gp_lr_apply_error_action == LR_APPLY_ERROR_RETRY)
			ReThrowError(edata);

		FreeErrorData(edata);
		lr_disable_subscription(tr, cfg->subid, message);
		ereport(ERROR,
				(errcode(sqlerrcode),
				 errmsg("%s", message),
				 errdetail("The initial synchronisation of subscription %u cannot succeed as things stand, so the subscription has been disabled rather than restarted for ever.",
						   cfg->subid),
				 errhint("Fix what the message names and re-enable the subscription, or set mpp_logicalrep.apply_error_action=retry to keep trying.")));
	}
	PG_END_TRY();
}

/* ------------------------------------------------------------------------
 * Waiting for libpq without going deaf
 * ------------------------------------------------------------------------
 *
 * PQexec() and PQconnectdb() sit in a blocking read until the far side
 * answers, running no CHECK_FOR_INTERRUPTS and looking at no flag of ours.
 * That matters here more than it does in an ordinary backend: this worker's
 * SIGTERM handler only raises mpp_worker_stop, which is read between units
 * of work, so a worker blocked in one of those calls -- gp_lr_drain waiting
 * for a batch, a connection to a coordinator that never answers -- is an
 * orphan that ignores every SIGTERM and only SIGKILL ends.  DROP
 * SUBSCRIPTION and DROP DATABASE wait behind it.
 *
 * So every statement of the worker is sent asynchronously and collected
 * here, the way the walreceiver's libpq wrapper does it
 * (libpqrcv_PQgetResult).  Only the waiting is borrowed from that wrapper,
 * not the wrapper itself: it connects with replication=database and nothing
 * else, such a backend is gp_role=utility, and every statement here has to
 * reach the segments -- gp_lr_drain(), gp_lr_apply(),
 * gp_create_logical_replication_slot(), COPY ... ON SEGMENT.  An ordinary
 * libpq connection is a QD and can dispatch them; a replication one answers
 * for the coordinator alone, and does so without complaining.
 *
 * The check made while waiting is deliberately the cheap one -- pending
 * interrupts and the signal flag.  Whether the subscription still wants this
 * worker is a catalog lookup (LrSubscriptionEnabled opens a transaction of
 * its own), which belongs between units of work, not in a loop that wakes
 * once per packet.
 */
static void
lr_pq_check_interrupts(void)
{
	CHECK_FOR_INTERRUPTS();
	if (LrWorkerStopRequested())
		ereport(ERROR,
				(errcode(ERRCODE_ADMIN_SHUTDOWN),
				 errmsg("terminating the apply worker on administrator command")));
}

/* sleep until the connection is ready for io_flag, or we are interrupted */
void
LrPQWaitIO(PGconn *conn, int io_flag)
{
	int			rc;

	lr_pq_check_interrupts();
	rc = WaitLatchOrSocket(MyLatch,
						   WL_EXIT_ON_PM_DEATH | WL_LATCH_SET | io_flag,
						   PQsocket(conn), 0, WAIT_EVENT_LOGICAL_APPLY_MAIN);
	if (rc & WL_LATCH_SET)
	{
		ResetLatch(MyLatch);
		lr_pq_check_interrupts();
	}
}

/*
 * The next result of a statement sent with PQsendQuery(), or NULL when there
 * are none left -- or when the connection broke, which the caller tells apart
 * by there being no result at all where one was due.
 */
PGresult *
LrPQGetResult(PGconn *conn)
{
	while (PQisBusy(conn))
	{
		LrPQWaitIO(conn, WL_SOCKET_READABLE);
		if (PQconsumeInput(conn) == 0)
			return NULL;
	}
	return PQgetResult(conn);
}

/*
 * Connect, stepping the handshake by hand so that the wait for a coordinator
 * that never answers is a wait we can be woken out of.  Returns a connection
 * whose status the caller still has to check, or throws if interrupted.
 */
PGconn *
LrPQConnect(const char *conninfo)
{
	PGconn	   *conn = PQconnectStart(conninfo);

	if (conn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory connecting to a coordinator")));

	while (PQstatus(conn) != CONNECTION_BAD)
	{
		PostgresPollingStatusType poll = PQconnectPoll(conn);

		if (poll == PGRES_POLLING_OK || poll == PGRES_POLLING_FAILED)
			break;
		if (PQsocket(conn) < 0)
			break;				/* no socket to wait on; the status tells why */

		/* the connection is ours alone until we return it */
		PG_TRY();
		{
			LrPQWaitIO(conn, poll == PGRES_POLLING_READING ? WL_SOCKET_READABLE
						   : WL_SOCKET_WRITEABLE);
		}
		PG_CATCH();
		{
			PQfinish(conn);
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	return conn;
}

/* ------------------------------------------------------------------------
 * The shared target session (mpp_apply_internal.h)
 *
 * One normal coordinator session, driven over libpq, that both halves of
 * the Cloudberry pairing use: the assembly runs gp_lr_connect and
 * gp_lr_drain on it, the sink runs gp_lr_apply and gp_lr_progress.  It has
 * to be one session -- the reader gang and the writer gang are slices of
 * the same dispatched plan.
 * ------------------------------------------------------------------------
 */

void
LrTransportConnect(LrCbdbTransport *tr)
{
	StringInfoData ci;
	PGresult   *res;

	/*
	 * connect_timeout goes first, so a subscription that names its own still
	 * wins; application_name goes last, so it cannot be overridden.
	 */
	initStringInfo(&ci);
	appendStringInfo(&ci, "connect_timeout=%d %s application_name=mpp_apply_worker options='-c gp_vmem_idle_resource_timeout=0'",
					 LR_CONNECT_TIMEOUT_S, tr->conninfo);
	tr->qd = LrPQConnect(ci.data);
	pfree(ci.data);
	if (PQstatus(tr->qd) != CONNECTION_OK)
	{
		char	   *msg = pstrdup(PQerrorMessage(tr->qd));

		PQfinish(tr->qd);
		tr->qd = NULL;
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the target coordinator: %s", msg)));
	}

	res = LrTransportExec(tr, "SELECT count(*) FROM gp_segment_configuration WHERE role = 'p' AND content >= 0");
	tr->nsegs = atoi(PQgetvalue(res, 0, 0));
	PQclear(res);
	if (tr->nsegs <= 0 || tr->nsegs > LR_MAX_SEGS)
		elog(ERROR, "unexpected number of target segments: %d", tr->nsegs);
}

void
LrTransportClose(LrCbdbTransport *tr)
{
	if (tr->qd != NULL)
		PQfinish(tr->qd);
	tr->qd = NULL;
}

PGresult *
LrTransportExec(LrCbdbTransport *tr, const char *sql)
{
	return LrTransportExecParams(tr, sql, 0, NULL);
}

PGresult *
LrTransportExecParams(LrCbdbTransport *tr, const char *sql, int nparams,
					  const char *const *params)
{
	PGresult   *res = NULL;
	PGresult   *r;
	bool		sent;

	sent = nparams > 0 ?
		PQsendQueryParams(tr->qd, sql, nparams, NULL, params, NULL, NULL, 0) != 0 :
		PQsendQuery(tr->qd, sql) != 0;

	/* keep the first result and drain the rest, so the session stays usable */
	if (sent)
		while ((r = LrPQGetResult(tr->qd)) != NULL)
		{
			if (res == NULL)
				res = r;
			else
				PQclear(r);
		}

	if (res == NULL ||
		(PQresultStatus(res) != PGRES_TUPLES_OK &&
		 PQresultStatus(res) != PGRES_COMMAND_OK))
	{
		char	   *msg = pstrdup(res ? PQresultErrorMessage(res) : PQerrorMessage(tr->qd));
		const char *state = res ? PQresultErrorField(res, PG_DIAG_SQLSTATE) : NULL;
		int			code = ERRCODE_CONNECTION_EXCEPTION;

		/*
		 * Carry the target's own SQLSTATE over.  Everything the session does
		 * comes back through here, so wrapping it all as a connection failure
		 * would tell the loop that every error is one that can pass (O6).
		 * Only a failure with no result at all really is one.
		 */
		if (state != NULL && strlen(state) == 5)
			code = MAKE_SQLSTATE(state[0], state[1], state[2], state[3], state[4]);

		if (res)
			PQclear(res);
		ereport(ERROR,
				(errcode(code),
				 errmsg("target coordinator: %s", msg),
				 errcontext("statement: %.200s", sql)));
	}
	return res;
}

/* nspname, relname -> "nsp"."rel", for a statement text */
char *
LrTransportQuoteRel(LrCbdbTransport *tr, const char *nspname, const char *relname)
{
	char	   *qn = PQescapeIdentifier(tr->qd, nspname, strlen(nspname));
	char	   *qr = PQescapeIdentifier(tr->qd, relname, strlen(relname));
	char	   *result = psprintf("%s.%s", qn, qr);

	PQfreemem(qn);
	PQfreemem(qr);
	return result;
}

/* keywords of a conninfo that are a secret rather than a coordinate */
static bool
conninfo_is_secret(const char *keyword)
{
	return strcmp(keyword, "password") == 0 ||
		strcmp(keyword, "sslpassword") == 0;
}

/*
 * Source segments: content -> conninfo.  Built from the source coordinator's
 * gp_segment_configuration with the caller's conninfo minus host/port -- and
 * minus its secrets, which do not travel in the CONNECT payload at all: that
 * payload is a bound parameter of a statement, and a server logging
 * statements writes its parameters out.  The readers take the secrets from
 * their own pg_subscription row instead, which is the same value, already on
 * the segment, and readable by nobody else.
 */
static void
driver_load_source_segments(LrDriver *d)
{
	WalReceiverConn *conn;
	char	   *err = NULL;
	WalRcvExecResult *res;
	Oid			types[3] = {INT4OID, TEXTOID, INT4OID};
	TupleTableSlot *slot;
	PQconninfoOption *opts;
	StringInfoData base;
	List	   *conninfos = NIL;
	int			maxcontent = -1;
	bool		started_xact = false;
	MemoryContext oldcxt;

	if (WalReceiverFunctions == NULL)
		libpqwalreceiver_PG_init();

	/* everything of the conninfo except host / hostaddr / port */
	opts = PQconninfoParse(d->cfg->source_conninfo, &err);
	if (opts == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid source conninfo: %s", err ? err : "?")));
	initStringInfo(&base);
	for (PQconninfoOption *o = opts; o->keyword; o++)
	{
		if (o->val == NULL || o->val[0] == '\0')
			continue;
		if (strcmp(o->keyword, "host") == 0 || strcmp(o->keyword, "hostaddr") == 0 ||
			strcmp(o->keyword, "port") == 0)
			continue;
		if (conninfo_is_secret(o->keyword))
		{
			d->src_has_secrets = true;
			continue;
		}
		appendStringInfo(&base, "%s='%s' ", o->keyword, o->val);
	}
	PQconninfoFree(opts);

	conn = walrcv_connect(d->cfg->source_conninfo, true, false, "mpp_apply_worker", &err);
	if (conn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the source coordinator: %s", err)));

	/*
	 * walrcv_exec builds the result's tuple descriptor, which looks up column
	 * types in the syscache and therefore needs an open transaction.  In the
	 * background-worker path there is none; start one for the duration of the
	 * query (the conninfos we keep are copied into d->cxt, so they survive the
	 * commit).  When a caller already has a transaction (the SQL test entry),
	 * leave its transaction alone.
	 */
	started_xact = !IsTransactionState();
	if (started_xact)
		StartTransactionCommand();

	res = walrcv_exec(conn,
					  "SELECT content, coalesce(address, hostname), port FROM gp_segment_configuration "
					  "WHERE role = 'p' AND content >= 0 ORDER BY content",
					  3, types);
	if (res->status != WALRCV_OK_TUPLES)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not read the source segment configuration: %s", res->err)));

	oldcxt = MemoryContextSwitchTo(d->cxt);
	slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
	while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
	{
		bool		isnull;
		int			content = DatumGetInt32(slot_getattr(slot, 1, &isnull));
		char	   *host = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
		int			port = DatumGetInt32(slot_getattr(slot, 3, &isnull));

		while (maxcontent < content)
		{
			conninfos = lappend(conninfos, NULL);
			maxcontent++;
		}
		lfirst(list_nth_cell(conninfos, content)) =
			psprintf("%shost='%s' port=%d", base.data, host, port);
		ExecClearTuple(slot);
	}
	MemoryContextSwitchTo(oldcxt);
	ExecDropSingleTupleTableSlot(slot);
	walrcv_clear_result(res);

	/*
	 * Which source relations are DISTRIBUTED REPLICATED?  Their rows live on
	 * every source segment, so every segment stream carries their changes and
	 * only one reader may emit them (mpp_source.c).  The list goes down with
	 * the CONNECT spec.
	 */
	{
		Oid			oidtype[1] = {OIDOID};
		List	   *repl = NIL;

		res = walrcv_exec(conn,
						  "SELECT localoid FROM gp_distribution_policy WHERE policytype = 'r'",
						  1, oidtype);
		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not read the source distribution policies: %s", res->err)));
		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		oldcxt = MemoryContextSwitchTo(d->cxt);
		while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		{
			bool		isnull;

			repl = lappend_oid(repl, DatumGetObjectId(slot_getattr(slot, 1, &isnull)));
			ExecClearTuple(slot);
		}
		MemoryContextSwitchTo(oldcxt);
		ExecDropSingleTupleTableSlot(slot);
		walrcv_clear_result(res);
		list_free(d->src_repl_relids);
		d->src_repl_relids = repl;
	}

	walrcv_disconnect(conn);

	if (started_xact)
		CommitTransactionCommand();

	d->seg_conninfos = conninfos;
	d->nsrcsegs = list_length(conninfos);

	if (d->nsrcsegs != d->tr->nsegs)
		elog(LOG, "mpp apply worker: source has %d segments, target %d: %s",
			 d->nsrcsegs, d->tr->nsegs,
			 d->nsrcsegs < d->tr->nsegs ? "some readers have no source" :
			 "some readers hold several streams");
}

/* CONNECT: readers connect from the progress positions */
static void
driver_connect_readers(LrDriver *d)
{
	StringInfoData spec;
	StringInfoData sql;
	ListCell   *lc;
	PGresult   *res;
	char		subid[16],
				token[32];
	const char *params[3];
	int			ncold = 0;

	initStringInfo(&spec);
	appendStringInfo(&spec, "slot=%s\npub=", d->cfg->seg_slot);
	foreach(lc, d->cfg->publications)
		appendStringInfo(&spec, "%s%s", lc == list_head(d->cfg->publications) ? "" : ",",
						 strVal(lfirst(lc)));
	appendStringInfoString(&spec, "\ntwophase=1\nstreaming=1\n");
	if (d->src_repl_relids != NIL)
	{
		appendStringInfoString(&spec, "srcrepl=");
		foreach(lc, d->src_repl_relids)
			appendStringInfo(&spec, "%s%u", lc == list_head(d->src_repl_relids) ? "" : ",",
							 lfirst_oid(lc));
		appendStringInfoChar(&spec, '\n');
	}
	/*
	 * Which source segments each reader serves.  Source segment s goes to
	 * reader s % N_dst, so the streams are spread evenly however the two
	 * segment counts compare: one each when they match, none for the readers
	 * beyond the source when the source is smaller, several per reader when
	 * it is larger.  Everything per stream --
	 * conninfo, flush, pos, applied -- is keyed by SOURCE segment.
	 */
	for (int i = 0; i < d->tr->nsegs; i++)
	{
		bool		first = true;

		appendStringInfo(&spec, "srcsegs.%d=", i);
		for (int s = i; s < d->nsrcsegs; s += d->tr->nsegs)
		{
			appendStringInfo(&spec, "%s%d", first ? "" : ",", s);
			first = false;
		}
		appendStringInfoChar(&spec, '\n');
	}
	if (d->src_has_secrets)
		appendStringInfoString(&spec, "secrets=1\n");
	for (int s = 0; s < d->nsrcsegs; s++)
	{
		appendStringInfo(&spec, "conninfo.%d=%s\n", s,
						 (char *) list_nth(d->seg_conninfos, s));
		appendStringInfo(&spec, "flush.%d=" INT64_FORMAT "\npos.%d=" INT64_FORMAT "\n",
						 s, d->seg_flush[s], s, d->seg_pos[s]);
		if (s < d->cfg->nsync_local && d->cfg->sync_local[s] != NULL)
			appendStringInfo(&spec, "localsnap.%d=%s\n", s, d->cfg->sync_local[s]);
	}

	/* applied gxids above seg_flush, per segment */
	snprintf(subid, sizeof(subid), "%u", d->cfg->subid);
	params[0] = subid;
	res = LrTransportExecParams(d->tr,
						 "SELECT seg, string_agg(gxid::text, ',' ORDER BY gxid) "
						 "FROM pg_catalog.gp_subscription_applied WHERE subid = $1 GROUP BY seg",
						 1, params);
	for (int i = 0; i < PQntuples(res); i++)
		appendStringInfo(&spec, "applied.%s=%s\n", PQgetvalue(res, i, 0), PQgetvalue(res, i, 1));
	PQclear(res);

	snprintf(token, sizeof(token), INT64_FORMAT, d->cfg->token);
	params[1] = token;
	params[2] = spec.data;
	initStringInfo(&sql);
	appendStringInfoString(&sql, "SELECT seg, kind FROM pg_catalog.gp_lr_connect($1, $2, $3)");
	res = LrTransportExecParams(d->tr, sql.data, 3, params);
	for (int i = 0; i < PQntuples(res); i++)
		if (PQgetvalue(res, i, 1)[0] != LR_REPORT_CONNECTED)
			ncold++;
	if (PQntuples(res) != d->tr->nsegs || ncold > 0)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("gp_lr_connect: %d of %d readers connected", PQntuples(res) - ncold, d->tr->nsegs)));
	PQclear(res);
	pfree(spec.data);
	pfree(sql.data);

	/* a fresh reader gang knows nothing of what we released before */
	d->release_gxids = NIL;
	d->release_locals = NIL;
	d->last_drain = GetCurrentTimestamp();
}

/* ------------------------------------------------------------------------
 * Source coordinator stream
 * ------------------------------------------------------------------------
 */

static void
driver_connect_source(LrDriver *d)
{
	char	   *err = NULL;
	WalRcvStreamOptions opts;
	ListCell   *lc;
	MemoryContext oldcxt = MemoryContextSwitchTo(d->cxt);

	d->src = walrcv_connect(d->cfg->source_conninfo, true, false, "mpp_apply_worker", &err);
	if (d->src == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the source coordinator: %s", err)));

	memset(&opts, 0, sizeof(opts));
	opts.logical = true;
	opts.slotname = pstrdup(d->cfg->coord_slot);
	opts.startpoint = (XLogRecPtr) d->coord_committed;	/* 0 = the slot's */
	opts.proto.logical.proto_version = LOGICALREP_PROTO_MAX_VERSION_NUM;
	foreach(lc, d->cfg->publications)
		opts.proto.logical.publication_names =
			lappend(opts.proto.logical.publication_names,
					makeString(pstrdup(strVal(lfirst(lc)))));
	opts.proto.logical.extra_options = "cbdb_events 'on'";
	if (!walrcv_startstreaming(d->src, &opts))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not start streaming from coordinator slot \"%s\"",
						d->cfg->coord_slot)));
	MemoryContextSwitchTo(oldcxt);

	d->coord_pos = d->coord_committed;
	d->last_feedback = GetCurrentTimestamp();
}

static void
driver_disconnect_source(LrDriver *d)
{
	if (d->src)
	{
		walrcv_disconnect(d->src);
		d->src = NULL;
	}
}

/*
 * Read the coordinator stream until nothing more is buffered or timeout_ms
 * elapsed.  Every dcommit event is handled as it arrives.  False = EOF.
 */
static bool
driver_pump_source(LrDriver *d, int timeout_ms)
{
	TimestampTz deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), timeout_ms);

	for (;;)
	{
		char	   *buf;
		int			len;
		pgsocket	fd = PGINVALID_SOCKET;
		long		remain;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Not connected: say the stream is gone rather than read from
		 * nothing.  The loop above turns that into its ordinary recovery,
		 * which is what reconnects.
		 */
		if (d->src == NULL)
			return false;

		len = walrcv_receive(d->src, &buf, &fd);
		if (len < 0)
			return false;
		if (len > 0)
		{
			StringInfoData s;

			s.data = buf;
			s.len = len;
			s.maxlen = -1;
			s.cursor = 0;
			d->rep.last_msg_time = GetCurrentTimestamp();
			switch (pq_getmsgbyte(&s))
			{
				case 'w':
					{
						XLogRecPtr	walend;
						char		type;

						(void) pq_getmsgint64(&s);	/* start */
						walend = pq_getmsgint64(&s);
						(void) pq_getmsgint64(&s);	/* send time */
						if ((int64) walend > d->coord_pos)
							d->coord_pos = walend;

						type = pq_getmsgbyte(&s);
						if (type == LOGICAL_REP_MSG_MESSAGE)
						{
							uint8		flags = pq_getmsgint(&s, 1);
							XLogRecPtr	lsn = pq_getmsgint64(&s);
							const char *prefix = pq_getmsgstring(&s);
							int			sz = pq_getmsgint(&s, 4);
							const char *content = pq_getmsgbytes(&s, sz);

							(void) flags;
							if (strcmp(prefix, LR_DCOMMIT_PREFIX) == 0)
							{
								/* gxid=<n> lsn=<n> segs=<a,b,..|*|-> exact=<0|1> */
								char	   *text = pnstrdup(content, sz);
								int64		gxid = 0;
								int16		segs[LR_MAX_SEGS];
								int			nsegs = 0;
								bool		exact = false;
								char	   *p;

								if ((p = strstr(text, "gxid=")) != NULL)
									gxid = strtoll(p + 5, NULL, 10);
								if ((p = strstr(text, "exact=")) != NULL)
								{
									exact = (p[6] == '1');
									*p = '\0';	/* keep it out of the seg list */
								}
								if ((p = strstr(text, "segs=")) != NULL && p[5] == '-')
									nsegs = -1;	/* known: no segment wrote */
								else if (p != NULL && p[5] != '*')
								{
									char	   *save = NULL;
									char	   *tok;

									for (tok = strtok_r(p + 5, ",", &save); tok && nsegs < LR_MAX_SEGS;
										 tok = strtok_r(NULL, ",", &save))
										segs[nsegs++] = (int16) atoi(tok);
								}
								pfree(text);
								if (gxid != 0)
									driver_handle_event(d, gxid, (int64) lsn, nsegs,
														segs, exact);
							}
						}
						/* the coordinator's own catalog transactions: ignored */
						break;
					}
				case 'k':
					{
						XLogRecPtr	walend = pq_getmsgint64(&s);
						bool		reply;

						(void) pq_getmsgint64(&s);
						reply = pq_getmsgbyte(&s);
						if ((int64) walend > d->coord_pos)
							d->coord_pos = walend;
						if (reply)
							driver_send_feedback(d);
						break;
					}
				default:
					break;
			}
			/* a batch may have become due while events kept coming */
			if (driver_batch_ready(d))
				return true;
			continue;
		}

		remain = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
		if (remain <= 0)
			return true;
		(void) WaitLatchOrSocket(MyLatch,
								 WL_LATCH_SET | WL_SOCKET_READABLE | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
								 fd, remain, WAIT_EVENT_LOGICAL_APPLY_MAIN);
		ResetLatch(MyLatch);
	}
}

/* standby status update on the coordinator slot: flush = last committed batch */
static void
driver_send_feedback(LrDriver *d)
{
	StringInfoData buf;

	if (d->src == NULL || d->coord_pos == 0)
		return;
	initStringInfo(&buf);
	pq_sendbyte(&buf, 'r');
	pq_sendint64(&buf, d->coord_pos);
	pq_sendint64(&buf, d->coord_committed);
	pq_sendint64(&buf, d->coord_committed);
	pq_sendint64(&buf, GetCurrentTimestamp());
	pq_sendbyte(&buf, 0);
	walrcv_send(d->src, buf.data, buf.len);
	pfree(buf.data);
	d->last_feedback = GetCurrentTimestamp();
}

/* ------------------------------------------------------------------------
 * Events, DRAIN and the registry
 * ------------------------------------------------------------------------
 */

static void
driver_handle_event(LrDriver *d, int64 gxid, int64 lsn, int nsegs, const int16 *segs,
					bool exact)
{
	LrGxidTxn  *t;
	MemoryContext oldcxt;

	d->stats->events++;

	t = registry_get(d, gxid, true);
	if (t->in_batch)
		return;					/* a replayed event of a batched transaction */
	t->has_event = true;
	t->event_lsn = lsn;
	if (nsegs < 0)
	{
		/*
		 * The coordinator knows no segment wrote anything (every prepared
		 * segment reported read-only): nothing to wait for or to apply.  It
		 * still takes its place in commit order (driver_collect_ready).
		 */
		t->noop = true;
		if (t->first_wait == 0)
			t->first_wait = GetCurrentTimestamp();
		driver_collect_ready(d);
		return;
	}
	if (nsegs > 0)
	{
		oldcxt = MemoryContextSwitchTo(d->cxt);
		t->nsegs_expected = nsegs;
		t->segs_exact = exact;
		t->segs_expected = palloc(sizeof(int16) * nsegs);
		memcpy(t->segs_expected, segs, sizeof(int16) * nsegs);
		MemoryContextSwitchTo(oldcxt);
	}
	if (t->first_wait == 0)
		t->first_wait = GetCurrentTimestamp();

	/*
	 * ② ask the readers.  A writing participant has already COMMIT PREPARED
	 * before the coordinator wrote this event, so its PREPARE is decodable
	 * now; a short wait catches it.  A participant that wrote nothing never
	 * sends the transaction at all (an empty prepared transaction leaves no
	 * segment stream), so we must not block for it: completion is decided by
	 * txn_complete() with a grace period, not by waiting here.
	 */
	driver_drain(d, gxid, nsegs, segs, Min(gp_lr_event_wait_ms, 500));

	/* dedup: readers said it is already applied */
	t = registry_get(d, gxid, false);
	if (t == NULL)
	{
		d->stats->events_deduped++;
		return;
	}

	if (!txn_complete(d, t))
		elog(DEBUG1, "mpp apply worker: gxid " INT64_FORMAT " not ready on every participant yet (%d of %d)",
			 gxid, t->nready, t->nsegs_expected);
	driver_collect_ready(d);
}

/*
 * Move every transaction that has its event and is ready on all its
 * participants into the batch, in event order.  Earlier events whose
 * segments were slow become complete on later drains; they are picked up
 * here.  One whose segments never report it stays registered and is warned
 * about once (it predates the segment slots, or a segment is far behind).
 */
/*
 * Initial-sync filter: is gxid already in the COPY taken at the cut point?
 * visible(G, DS_t) = G < xmax AND G not in in_progress.
 */
static bool
gxid_covered_by_sync(LrDriver *d, int64 gxid)
{
	if (d->cfg->sync_ds_xmax == 0 || gxid == 0 || gxid >= d->cfg->sync_ds_xmax)
		return false;
	for (int i = 0; i < d->cfg->nsync_ds_inprogress; i++)
		if (d->cfg->sync_ds_inprogress[i] == gxid)
			return false;		/* in progress at the cut => not yet copied */
	return true;
}

static void
driver_collect_ready(LrDriver *d)
{
	HASH_SEQ_STATUS seq;
	LrGxidTxn  *t;
	LrGxidTxn **arr;
	int			n = 0;
	int			cap;
	MemoryContext oldcxt;

	/*
	 * Apply strictly in commit (coordinator event) order.  A transaction is
	 * applied only after every earlier one has been applied or shown to be a
	 * no-op, so a fast later transaction can never overtake a slower earlier
	 * one (e.g. a one-phase DELETE must not run before the two-phase INSERT
	 * of the same row, which may wait out the grace period because a
	 * dispatched-but-empty segment never reports it).
	 */
	cap = hash_get_num_entries(d->gxids);
	if (cap == 0)
		return;
	arr = palloc(sizeof(LrGxidTxn *) * cap);
	hash_seq_init(&seq, d->gxids);
	while ((t = (LrGxidTxn *) hash_seq_search(&seq)) != NULL)
		if (!t->in_batch && t->has_event)
			arr[n++] = t;

	/* sort by event position (commit order) */
	for (int a = 1; a < n; a++)
		for (int b = a; b > 0 && arr[b - 1]->event_lsn > arr[b]->event_lsn; b--)
		{
			LrGxidTxn  *tmp = arr[b];

			arr[b] = arr[b - 1];
			arr[b - 1] = tmp;
		}

	oldcxt = MemoryContextSwitchTo(d->batchcxt);
	for (int i = 0; i < n; i++)
	{
		t = arr[i];
		if (t->noop && t->nready == 0)
		{
			/* known no-op: advance past it in order, nothing to release */
			MemoryContextSwitchTo(d->cxt);
			if (t->event_lsn > d->coord_committed)
				d->coord_committed = t->event_lsn;
			d->stats->events_noop++;
			registry_remove(d, t);
			MemoryContextSwitchTo(d->batchcxt);
			continue;
		}
		if (txn_complete(d, t))
		{
			/*
			 * Complete but reported by nobody: it changed nothing this
			 * subscription replicates (or its segment parts predate the
			 * segment slots).  Advance past it in commit order.
			 */
			if (t->nready == 0)
			{
				MemoryContextSwitchTo(d->cxt);
				if (t->event_lsn > d->coord_committed)
					d->coord_committed = t->event_lsn;
				d->stats->events_deduped++;
				registry_remove(d, t);
				MemoryContextSwitchTo(d->batchcxt);
				continue;
			}

			/*
			 * Already in the initial-sync COPY: drop it (release the reader
			 * buffer, advance past it) rather than applying it again.  Order
			 * is preserved because a dropped no-op cannot depend on or be
			 * depended upon by later transactions.
			 */
			if (gxid_covered_by_sync(d, t->gxid))
			{
				MemoryContextSwitchTo(d->cxt);
				if (t->event_lsn > d->coord_committed)
					d->coord_committed = t->event_lsn;
				{
					int64	   *g = palloc(sizeof(int64));

					*g = t->gxid;
					d->release_gxids = lappend(d->release_gxids, g);
				}
				d->stats->events_deduped++;
				registry_remove(d, t);
				MemoryContextSwitchTo(d->batchcxt);
				continue;
			}
			if (d->batch_gxids == NIL && d->batch_locals == NIL)
				d->batch_started = GetCurrentTimestamp();
			d->batch_gxids = lappend(d->batch_gxids, t);
			t->in_batch = true;
			d->batch_bytes += t->nbytes;
			continue;
		}
		/*
		 * This transaction is not ready yet and is not a proven no-op.  It is
		 * the earliest such one; nothing after it may be applied before it.
		 */
		break;
	}
	MemoryContextSwitchTo(oldcxt);
	pfree(arr);
}

/*
 * Has this segment's stream already moved past where the transaction would
 * be?  A later transaction (later in the coordinator's commit order) that
 * the same segment has reported proves it: segment streams arrive in commit
 * order, so anything the transaction wrote there would have come first.
 * That is how a transaction whose segment part predates the segment slot --
 * the transaction that created the slots, say -- is recognised instead of
 * waited for forever.
 */
static bool
seg_moved_past(LrDriver *d, LrGxidTxn *t, int seg)
{
	HASH_SEQ_STATUS seq;
	LrGxidTxn  *u;

	hash_seq_init(&seq, d->gxids);
	while ((u = (LrGxidTxn *) hash_seq_search(&seq)) != NULL)
	{
		if (u == t || !u->has_event || u->event_lsn <= t->event_lsn)
			continue;
		if (u->parts[seg].ready)
		{
			hash_seq_term(&seq);
			return true;
		}
	}
	return false;
}

/*
 * Is the transaction ready on every participant?
 *
 * The participant list of an exact event (S4b) names the segments that
 * wrote, and each of them does send the transaction, so a participant that
 * has not reported yet is simply behind: wait for it rather than apply half
 * the transaction -- unless its stream is proven to be past it
 * (seg_moved_past), or the wait has become so long that progress matters
 * more than completeness, which is then warned about.
 *
 * A list that is not exact is the set of segments the transaction was
 * dispatched to; those that wrote nothing never report, so there the short
 * grace period decides.  Without any list we wait for every segment.
 */
static bool
txn_complete(LrDriver *d, LrGxidTxn *t)
{
	bool		late;

	if (!t->has_event)
		return false;

	late = TimestampDifferenceExceeds(t->first_wait, GetCurrentTimestamp(),
									  (t->segs_exact ? 10 : 1) * gp_lr_event_wait_ms);

	/* without a participant list, every SOURCE segment has to report */
	if (t->nsegs_expected <= 0)
		return t->nready == d->nsrcsegs || (t->nready > 0 && late);

	for (int i = 0; i < t->nsegs_expected; i++)
	{
		int			seg = t->segs_expected[i];

		if (seg < 0 || seg >= d->nsrcsegs || t->parts[seg].ready)
			continue;
		if (t->segs_exact && seg_moved_past(d, t, seg))
			continue;			/* that stream is past it: it wrote nothing we replicate */
		if (!late)
			return false;
		if (!t->warned)
		{
			t->warned = true;
			elog(WARNING, "mpp apply worker: gxid " INT64_FORMAT " applied without segment %d, which has not reported it after %d ms (%d of %d participants reported)",
				 t->gxid, seg, (t->segs_exact ? 10 : 1) * gp_lr_event_wait_ms,
				 t->nready, t->nsegs_expected);
		}
	}
	return true;
}

/*
 * gp_lr_drain: release the last batch, read the streams, absorb the
 * reports.  gxid 0 = idle poll.
 */
static void
driver_drain(LrDriver *d, int64 gxid, int nsegs, const int16 *segs, int wait_ms)
{
	StringInfoData parts,
				relg,
				rell;
	char		subid[16],
				token[32],
				gxidbuf[32],
				waitbuf[16];
	const char *params[7];
	PGresult   *res;
	ListCell   *lc;
	MemoryContext oldcxt;

	snprintf(subid, sizeof(subid), "%u", d->cfg->subid);
	snprintf(token, sizeof(token), INT64_FORMAT, d->cfg->token);
	snprintf(gxidbuf, sizeof(gxidbuf), INT64_FORMAT, gxid);
	snprintf(waitbuf, sizeof(waitbuf), "%d", wait_ms);

	initStringInfo(&parts);
	if (nsegs > 0)
	{
		appendStringInfoChar(&parts, '{');
		for (int i = 0; i < nsegs; i++)
			appendStringInfo(&parts, "%s%d", i ? "," : "", segs[i]);
		appendStringInfoChar(&parts, '}');
	}
	initStringInfo(&relg);
	appendStringInfoChar(&relg, '{');
	foreach(lc, d->release_gxids)
		appendStringInfo(&relg, "%s" INT64_FORMAT, lc == list_head(d->release_gxids) ? "" : ",",
						 *(int64 *) lfirst(lc));
	appendStringInfoChar(&relg, '}');
	initStringInfo(&rell);
	appendStringInfoChar(&rell, '{');
	foreach(lc, d->release_locals)
	{
		LrLocalTxn *l = (LrLocalTxn *) lfirst(lc);

		appendStringInfo(&rell, "%s\"%d:" INT64_FORMAT "\"", lc == list_head(d->release_locals) ? "" : ",",
						 l->seg, l->seg_lsn);
	}
	appendStringInfoChar(&rell, '}');

	params[0] = subid;
	params[1] = token;
	params[2] = gxidbuf;
	params[3] = nsegs > 0 ? parts.data : NULL;
	params[4] = waitbuf;
	params[5] = relg.data;
	params[6] = rell.data;
	res = LrTransportExecParams(d->tr,
						 "SELECT seg, kind, gxid, seg_lsn, array_to_string(rel_names, E'\\n'), lockmode, nrows, nbytes, "
						 "array_to_string(remote_relids, ',') "
						 "FROM pg_catalog.gp_lr_drain($1, $2, $3, $4::int4[], $5::int4, $6::int8[], $7::text[])",
						 7, params);

	/* the release went through with this call */
	d->release_gxids = NIL;
	d->release_locals = NIL;
	d->last_drain = GetCurrentTimestamp();

	oldcxt = MemoryContextSwitchTo(d->cxt);
	for (int i = 0; i < PQntuples(res); i++)
	{
		int			seg = atoi(PQgetvalue(res, i, 0));
		char		kind = PQgetvalue(res, i, 1)[0];
		int64		rgxid = strtoll(PQgetvalue(res, i, 2), NULL, 10);
		int64		seg_lsn = strtoll(PQgetvalue(res, i, 3), NULL, 10);
		char	   *names = PQgetvalue(res, i, 4);
		int			lockmode = atoi(PQgetvalue(res, i, 5));
		int64		nrows = strtoll(PQgetvalue(res, i, 6), NULL, 10);
		int64		nbytes = strtoll(PQgetvalue(res, i, 7), NULL, 10);
		List	   *rel_names = NIL;

		/*
		 * Every report but COLD carries the SOURCE segment it is about; a
		 * cold reader has no stream to name, so it reports itself.
		 */
		if (kind != LR_REPORT_COLD && (seg < 0 || seg >= d->nsrcsegs))
			continue;

		if (kind == LR_REPORT_READY || kind == LR_REPORT_LOCAL)
		{
			char	   *copy = pstrdup(names);
			char	   *save = NULL;
			char	   *tok;

			for (tok = strtok_r(copy, "\n", &save); tok; tok = strtok_r(NULL, "\n", &save))
				rel_names = lappend(rel_names, pstrdup(tok));

			/* remember which remote relid each name has, for defer= */
			note_relnames(d, PQgetvalue(res, i, 8), names);
		}

		switch (kind)
		{
			case LR_REPORT_READY:
				{
					LrGxidTxn  *t = registry_get(d, rgxid, true);
					ListCell   *nc;

					if (!t->parts[seg].ready)
					{
						t->parts[seg].ready = true;
						t->nready++;
					}
					t->parts[seg].seg_lsn = seg_lsn;
					t->parts[seg].nrows = nrows;
					t->parts[seg].nbytes = nbytes;
					t->nbytes += nbytes;
					t->lockmode = Max(t->lockmode, lockmode);
					foreach(nc, rel_names)
					{
						bool		dup = false;
						ListCell   *oc;

						foreach(oc, t->rel_names)
							if (strcmp((char *) lfirst(oc), (char *) lfirst(nc)) == 0)
								dup = true;
						if (!dup)
							t->rel_names = lappend(t->rel_names, lfirst(nc));
					}
					break;
				}
			case LR_REPORT_LOCAL:
				{
					LrLocalTxn *l = palloc0(sizeof(LrLocalTxn));

					l->seg = seg;
					l->xid = rgxid;		/* a LOCAL report states its xid here */
					l->seg_lsn = seg_lsn;
					l->rel_names = rel_names;
					l->lockmode = lockmode;
					l->nbytes = nbytes;
					d->locals = lappend(d->locals, l);
					break;
				}
			case LR_REPORT_STATUS:
				if (seg_lsn > d->seg_pos[seg])
					d->seg_pos[seg] = seg_lsn;
				/* what that reader is still holding, for O2 */
				if (seg < LR_STAT_MAX_SEGS)
				{
					d->rep.seg_buffered_txns[seg] = nrows;
					d->rep.seg_buffered_bytes[seg] = nbytes;
				}
				break;
			case LR_REPORT_DEDUP:
				{
					LrGxidTxn  *t = registry_get(d, rgxid, false);

					elog(LOG, "mpp apply worker: source segment %d says gxid " INT64_FORMAT " is already applied",
						 seg, rgxid);
					if (t && !t->in_batch)
						registry_remove(d, t);
					break;
				}
			case LR_REPORT_COLD:
				PQclear(res);
				MemoryContextSwitchTo(oldcxt);
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("reader of segment %d is cold", seg)));
			default:
				break;
		}
	}
	MemoryContextSwitchTo(oldcxt);
	PQclear(res);
	pfree(parts.data);
	pfree(relg.data);
	pfree(rell.data);

	/* local transactions join the batch as they come */
	if (d->locals != NIL)
	{
		oldcxt = MemoryContextSwitchTo(d->batchcxt);
		if (d->batch_gxids == NIL && d->batch_locals == NIL)
			d->batch_started = GetCurrentTimestamp();
		foreach(lc, d->locals)
		{
			d->batch_locals = lappend(d->batch_locals, lfirst(lc));
			d->batch_bytes += ((LrLocalTxn *) lfirst(lc))->nbytes;
		}
		list_free(d->locals);
		d->locals = NIL;
		MemoryContextSwitchTo(oldcxt);
	}
}

static LrGxidTxn *
registry_get(LrDriver *d, int64 gxid, bool create)
{
	LrGxidTxn  *t;
	bool		found;

	if (!create)
		return (LrGxidTxn *) hash_search(d->gxids, &gxid, HASH_FIND, NULL);
	t = (LrGxidTxn *) hash_search(d->gxids, &gxid, HASH_ENTER, &found);
	if (!found)
	{
		memset(t, 0, sizeof(LrGxidTxn));
		t->gxid = gxid;
		t->parts = MemoryContextAllocZero(d->cxt, sizeof(LrSegPart) * d->nsrcsegs);
	}
	return t;
}

static void
registry_remove(LrDriver *d, LrGxidTxn *t)
{
	pfree(t->parts);
	if (t->segs_expected)
		pfree(t->segs_expected);
	list_free(t->rel_names);
	hash_search(d->gxids, &t->gxid, HASH_REMOVE, NULL);
}

/* ------------------------------------------------------------------------
 * Batches
 * ------------------------------------------------------------------------
 */

static bool
driver_batch_ready(LrDriver *d)
{
	if (d->batch_gxids == NIL && d->batch_locals == NIL)
		return false;
	if (list_length(d->batch_gxids) >= gp_lr_batch_max_txns)
		return true;
	if (d->batch_bytes >= (int64) gp_lr_batch_max_bytes * 1024)
		return true;
	/* a TRUNCATE is applied alone (AccessExclusiveLock) */
	if (list_length(d->batch_gxids) == 1 &&
		((LrGxidTxn *) linitial(d->batch_gxids))->lockmode >= AccessExclusiveLock)
		return true;
	return TimestampDifferenceExceeds(d->batch_started, GetCurrentTimestamp(),
									  gp_lr_batch_max_latency_ms);
}

/* "nsp.rel" -> its two parts, or false if it is not qualified */
static bool
split_relname(const char *qualified, char **nspname, char **relname)
{
	const char *dot = strchr(qualified, '.');

	if (dot == NULL)
		return false;
	*nspname = pnstrdup(qualified, dot - qualified);
	*relname = pstrdup(dot + 1);
	return true;
}

/*
 * Add "nsp.rel" to the relations of the unit being built, keeping the
 * strongest lock anything in the unit needs on it.
 */
static void
unit_add_rel(LrDriver *d, const char *qualified, int lockmode)
{
	LogicalRepUnit *u = &d->unit;
	char	   *nsp,
			   *rel;

	if (!split_relname(qualified, &nsp, &rel))
		return;
	for (int i = 0; i < u->nrels; i++)
	{
		if (strcmp(u->rels[i].rel.nspname, nsp) == 0 &&
			strcmp(u->rels[i].rel.relname, rel) == 0)
		{
			u->rels[i].lockmode = Max(u->rels[i].lockmode, lockmode);
			return;
		}
	}
	u->rels[u->nrels].rel.nspname = nsp;
	u->rels[u->nrels].rel.relname = rel;
	u->rels[u->nrels].rel.remote_relid = InvalidOid;
	u->rels[u->nrels].lockmode = lockmode;
	u->nrels++;
}
/* ------------------------------------------------------------------------
 * Tables added to a running subscription
 *
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION writes a gp_subscription_rel
 * row in state 'i' for every new table.  From the moment the driver sees
 * such a row it stops emitting that table's changes (defer= in the APPLY
 * plan) and keeps the transactions that carry them buffered in the readers.
 * The copy then runs under a cut point of its own, stepped from the apply
 * loop so the other tables keep streaming.  When it is done the held
 * transactions are replayed for that table alone (catchup=), dropping the
 * ones the copy already contains, and only then does the table become
 * ready.
 * ------------------------------------------------------------------------
 */

static bool
name_in_list(List *names, const char *name)
{
	ListCell   *lc;

	foreach(lc, names)
		if (strcmp((char *) lfirst(lc), name) == 0)
			return true;
	return false;
}

static bool
name_is_deferred(LrDriver *d, const char *name)
{
	return d->defer_names != NIL && name_in_list(d->defer_names, name);
}

static bool
txn_is_deferred(LrDriver *d, List *rel_names)
{
	ListCell   *lc;

	if (d->defer_names == NIL)
		return false;
	foreach(lc, rel_names)
		if (name_in_list(d->defer_names, (char *) lfirst(lc)))
			return true;
	return false;
}

/* remember the remote relid of each name a report carried */
static void
note_relnames(LrDriver *d, const char *relids, const char *names)
{
	char	   *rcopy;
	char	   *ncopy;
	char	   *rsave = NULL;
	char	   *nsave = NULL;
	char	   *rtok;
	char	   *ntok;
	MemoryContext oldcxt;

	if (relids == NULL || names == NULL || *relids == '\0')
		return;
	oldcxt = MemoryContextSwitchTo(d->cxt);
	rcopy = pstrdup(relids);
	ncopy = pstrdup(names);
	rtok = strtok_r(rcopy, ",", &rsave);
	ntok = strtok_r(ncopy, "\n", &nsave);
	while (rtok != NULL && ntok != NULL)
	{
		Oid			relid = (Oid) strtoul(rtok, NULL, 10);
		ListCell   *lc;
		bool		found = false;

		foreach(lc, d->relnames)
		{
			LrRelName  *rn = (LrRelName *) lfirst(lc);

			if (rn->relid == relid)
			{
				if (strcmp(rn->name, ntok) != 0)
					rn->name = pstrdup(ntok);
				found = true;
				break;
			}
		}
		if (!found && strcmp(ntok, "?") != 0)
		{
			LrRelName  *rn = palloc(sizeof(LrRelName));

			rn->relid = relid;
			rn->name = pstrdup(ntok);
			d->relnames = lappend(d->relnames, rn);
		}
		rtok = strtok_r(NULL, ",", &rsave);
		ntok = strtok_r(NULL, "\n", &nsave);
	}
	pfree(rcopy);
	pfree(ncopy);
	MemoryContextSwitchTo(oldcxt);
}

/* the remote relids of these table names, as an array literal, or NULL */
static char *
relid_set(LrDriver *d, List *names)
{
	StringInfoData buf;
	ListCell   *lc;

	initStringInfo(&buf);
	appendStringInfoChar(&buf, '{');
	foreach(lc, d->relnames)
	{
		LrRelName  *rn = (LrRelName *) lfirst(lc);

		if (!name_in_list(names, rn->name))
			continue;
		appendStringInfo(&buf, "%s%u", buf.len > 1 ? "," : "", rn->relid);
	}
	appendStringInfoChar(&buf, '}');
	if (buf.len == 2)
	{
		pfree(buf.data);
		return NULL;			/* none of them is in the stream yet */
	}
	return buf.data;
}

/*
 * Which tables of this subscription may not receive changes yet?  Cheap
 * enough to poll: one indexed read of a coordinator-only table.
 */
static void
driver_check_pending(LrDriver *d, bool force)
{
	List	   *names;
	MemoryContext oldcxt;
	bool		changed;

	if (!force &&
		!TimestampDifferenceExceeds(d->last_pending_check, GetCurrentTimestamp(),
									LR_PENDING_CHECK_MS))
		return;
	d->last_pending_check = GetCurrentTimestamp();

	oldcxt = MemoryContextSwitchTo(d->cxt);
	names = LrSyncPendingNames(d->tr->qd, d->cfg->subid);
	changed = (list_length(names) != list_length(d->defer_names));
	if (!changed)
	{
		ListCell   *lc;

		foreach(lc, names)
			if (!name_in_list(d->defer_names, (char *) lfirst(lc)))
				changed = true;
	}
	if (changed)
	{
		StringInfoData buf;
		ListCell   *lc;

		initStringInfo(&buf);
		foreach(lc, names)
			appendStringInfo(&buf, "%s%s", buf.len ? ", " : "", (char *) lfirst(lc));
		elog(LOG, "mpp apply worker: subscription %u: %d table(s) not ready: %s",
			 d->cfg->subid, list_length(names), buf.len ? buf.data : "-");
		pfree(buf.data);
	}
	list_free_deep(d->defer_names);
	d->defer_names = names;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Has everything the source committed up to `lsn` been applied?
 *
 * Normally the answer is the position of the last committed unit.  When the
 * source falls quiet that position stops moving even though nothing is
 * outstanding, so an idle stream that has been seen past `lsn` with an empty
 * registry counts as well -- otherwise a quiet moment would hold an added
 * table pending for ever.
 */
static bool
stream_applied_through(LrDriver *d, int64 lsn)
{
	if (lsn == 0 || d->coord_committed >= lsn)
		return true;

	return d->coord_pos >= lsn &&
		hash_get_num_entries(d->gxids) == 0 &&
		d->batch_gxids == NIL &&
		d->batch_locals == NIL &&
		d->locals == NIL;
}

/*
 * "xmin:xmax:xip,..." as gp_lr_local_snapshot() states it -> one source
 * segment's snapshot.  The values are epoch-extended; the stream carries
 * plain 32-bit xids, so the low half is what is compared, which is right for
 * as long as the filter matters (it stops matching a wraparound away, and
 * stops mattering long before that).
 */
static void
parse_local_snap(const char *value, LrLocalSnap *snap)
{
	char	   *copy;
	char	   *save = NULL;
	char	   *tok;
	char	   *xip;
	List	   *xids = NIL;
	ListCell   *lc;
	int			i = 0;

	memset(snap, 0, sizeof(*snap));
	if (value == NULL || *value == '\0')
		return;
	copy = pstrdup(value);
	if ((tok = strtok_r(copy, ":", &save)) == NULL)
		return;
	snap->xmin = (TransactionId) strtoull(tok, NULL, 10);
	if ((tok = strtok_r(NULL, ":", &save)) == NULL)
	{
		snap->xmin = InvalidTransactionId;
		return;					/* no xmax: the snapshot stays unusable */
	}
	snap->xmax = (TransactionId) strtoull(tok, NULL, 10);

	/* the rest of the string, if any, is the in-flight list */
	xip = strtok_r(NULL, ":", &save);
	for (tok = xip ? strtok_r(xip, ",", &save) : NULL; tok != NULL;
		 tok = strtok_r(NULL, ",", &save))
	{
		TransactionId *v = palloc(sizeof(TransactionId));

		*v = (TransactionId) strtoull(tok, NULL, 10);
		xids = lappend(xids, v);
	}
	snap->nxip = list_length(xids);
	if (snap->nxip == 0)
		return;
	snap->xip = palloc(sizeof(TransactionId) * snap->nxip);
	foreach(lc, xids)
		snap->xip[i++] = *(TransactionId *) lfirst(lc);
}

/*
 * Take the cut point of the job that has just started: the distributed
 * snapshot, which tells which held gxid the copy already contains, and the
 * per-segment local one, which answers the same for a utility write.  Both
 * are read for as long as the job's transactions are held, so both are
 * allocated in the caller's context, which is the run's.
 */
static void
driver_take_add_snapshot(LrDriver *d)
{
	char	  **text;
	int			ntext;

	LrSyncAddSnapshot(d->sync, &d->sync_ds_xmax, &d->sync_nds_inprogress,
					  &d->sync_ds_inprogress, &ntext, &text);
	d->sync_nlocal = 0;
	d->sync_local = NULL;
	if (ntext <= 0)
		return;
	d->sync_local = (LrLocalSnap *) palloc0(sizeof(LrLocalSnap) * ntext);
	d->sync_nlocal = ntext;
	for (int seg = 0; seg < ntext; seg++)
		parse_local_snap(text[seg], &d->sync_local[seg]);
}

/*
 * Start, step and complete the incremental job.  Called once per iteration
 * of the apply loop, and never blocks for longer than one libpq round trip.
 *
 * When the copy is done the assembly switches to catch-up: from then on it
 * hands out nothing but the held transactions, until they are all replayed
 * and the tables can go live.
 */
static bool
driver_sync_progress(LrDriver *d)
{
	MemoryContext oldcxt;

	if (d->catchup_mode)
	{
		if (d->deferred_txns != NIL)
			return true;		/* more catch-up units to hand out */

		/* the readers get the last buffers back, then the tables go live */
		driver_drain(d, 0, 0, NULL, 0);
		LrSyncAddFinish(d->sync);
		d->sync = NULL;
		list_free_deep(d->sync_names);
		d->sync_names = NIL;
		d->sync_ds_xmax = 0;
		d->sync_nds_inprogress = 0;
		d->sync_nlocal = 0;
		d->sync_local = NULL;
		d->catchup_mode = false;
		driver_check_pending(d, true);
		return false;
	}

	if (d->sync == NULL)
	{
		driver_check_pending(d, false);
		if (d->defer_names == NIL)
			return false;
		d->sync = LrSyncAddStart(d->mycfg);
		if (d->sync == NULL)
			return false;		/* the rows went away under us */
		oldcxt = MemoryContextSwitchTo(d->cxt);
		d->sync_names = LrSyncAddNames(d->sync);
		driver_take_add_snapshot(d);
		MemoryContextSwitchTo(oldcxt);
		return false;
	}

	if (!LrSyncAddStep(d->sync))
		return false;

	/*
	 * Copied -- but the tables may not go live yet.  Everything the copy
	 * contains committed before its cut point, and a transaction that
	 * committed before the cut point has not necessarily reached the apply
	 * loop: the stream runs behind.  If the tables went live now, such a
	 * transaction would be applied to them with nothing holding its rows
	 * back, and every row of it is in the copy already (r12: a duplicate key
	 * on the last row the copy took).  So keep streaming -- with the tables
	 * still deferred, which is what puts those transactions in the held set
	 * -- until the applied position has passed the cut point.
	 */
	if (!stream_applied_through(d, LrSyncAddCutLsn(d->sync)))
		return false;

	/*
	 * Now everything the copy contains is either applied and held or already
	 * dropped.  Replay what was held, for these tables only.  Saying so lets
	 * the driver drain them and come back here without reading the sources
	 * in between, so nothing that arrives meanwhile slips in between the
	 * catch-up and the moment the tables go live.
	 */
	d->catchup_mode = true;
	return true;
}

/* ------------------------------------------------------------------------
 * The Cloudberry assembly: a LogicalRepAssembly
 *
 * Everything above builds it; this is the face it turns to the driver.
 * The driver asks it to read the sources, hands the units it produces to
 * the sink, and tells it when one is committed.  What a unit contains --
 * how many transactions, which relations, which stream positions -- is the
 * assembly's business; what it costs to apply one is the sink's.
 *
 * The two are a pair here, because the rows never reach this process: the
 * sink's capabilities say delivers_rows is off, so the assembly leaves the
 * rows where they are (in the reader gang) and only names the transactions
 * the writers should replay.
 * ------------------------------------------------------------------------
 */

/*
 * The same question for a utility write, which has no gxid: its own segment
 * froze a snapshot at the cut point, and a transaction visible under that
 * one had committed before the copy read the table.
 */
static bool
local_covered_by_add_sync(LrDriver *d, int seg, int64 xid64)
{
	TransactionId xid = (TransactionId) xid64;
	const LrLocalSnap *snap;

	if (seg < 0 || seg >= d->sync_nlocal)
		return false;
	snap = &d->sync_local[seg];
	if (!TransactionIdIsValid(snap->xmax) || !TransactionIdIsValid(xid))
		return false;
	if (!TransactionIdPrecedes(xid, snap->xmax))
		return false;			/* started after the cut => not copied */
	if (TransactionIdPrecedes(xid, snap->xmin))
		return true;
	for (int i = 0; i < snap->nxip; i++)
		if (TransactionIdEquals(xid, snap->xip[i]))
			return false;		/* in flight at the cut => not copied */
	return true;
}

/* is this held transaction already inside the copy of the running job? */
static bool
gxid_covered_by_add_sync(LrDriver *d, int64 gxid)
{
	if (d->sync_ds_xmax == 0 || gxid == 0 || gxid >= d->sync_ds_xmax)
		return false;
	for (int i = 0; i < d->sync_nds_inprogress; i++)
		if (d->sync_ds_inprogress[i] == gxid)
			return false;		/* in progress at the cut => not yet copied */
	return true;
}

/*
 * The closed batch becomes one unit: its transactions in commit order, with
 * each one's position in each participating segment's stream, and (only if
 * the sink asked) the relations it writes.
 */
static bool
unit_build_from_batch(LrDriver *d)
{
	LogicalRepUnit *u = &d->unit;
	ListCell   *lc;
	int			ngx = list_length(d->batch_gxids);
	int			nlocal = list_length(d->batch_locals);
	int			maxrels = 0;
	int			n = 0;
	MemoryContext oldcxt = MemoryContextSwitchTo(d->batchcxt);

	memset(u, 0, sizeof(*u));
	memset(&d->unit_spec, 0, sizeof(d->unit_spec));
	d->unit_coord_lsn = d->coord_committed;
	d->unit_batch = NIL;
	d->unit_dropped = 0;

	u->txns = palloc0(sizeof(AssembledTxn) * Max(ngx + nlocal, 1));

	foreach(lc, d->batch_gxids)
	{
		LrGxidTxn  *t = (LrGxidTxn *) lfirst(lc);
		AssembledTxn *at = &u->txns[n++];
		int			np = 0;

		if (t->event_lsn > d->unit_coord_lsn)
			d->unit_coord_lsn = t->event_lsn;

		at->gxid = (uint64) t->gxid;
		at->commit_order_pos = (XLogRecPtr) t->event_lsn;
		at->participant_segidx = palloc(sizeof(int) * d->nsrcsegs);
		at->participant_pos = palloc(sizeof(XLogRecPtr) * d->nsrcsegs);
		for (int seg = 0; seg < d->nsrcsegs; seg++)
		{
			if (!t->parts[seg].ready)
				continue;
			at->participant_segidx[np] = seg;
			at->participant_pos[np] = (XLogRecPtr) t->parts[seg].seg_lsn;
			np++;
		}
		at->nparticipants = np;
		maxrels += list_length(t->rel_names);
	}

	/*
	 * A utility write has no distributed identity: it is one segment's
	 * transaction and its position in that segment's stream is all there is
	 * of it, so it always carries its one participant.
	 */
	foreach(lc, d->batch_locals)
	{
		LrLocalTxn *l = (LrLocalTxn *) lfirst(lc);
		AssembledTxn *at = &u->txns[n++];

		at->gxid = 0;
		at->commit_order_pos = (XLogRecPtr) l->seg_lsn;
		at->nparticipants = 1;
		at->participant_segidx = palloc(sizeof(int));
		at->participant_pos = palloc(sizeof(XLogRecPtr));
		at->participant_segidx[0] = l->seg;
		at->participant_pos[0] = (XLogRecPtr) l->seg_lsn;
		maxrels += list_length(l->rel_names);
	}
	u->ntxns = n;
	u->commit_order_pos = (XLogRecPtr) d->unit_coord_lsn;

	/*
	 * The relations, for the coordinator to lock before the writers do.  A
	 * table that is still being copied is left out: this unit does not write
	 * it, and taking its lock would fight the copy for it.
	 */
	if (d->caps.needs_relid_enumeration_for_lock)
	{
		u->rels = palloc(sizeof(LogicalRepRelLock) * Max(maxrels, 1));
		foreach(lc, d->batch_gxids)
		{
			LrGxidTxn  *t = (LrGxidTxn *) lfirst(lc);
			ListCell   *nc;

			foreach(nc, t->rel_names)
				if (!name_is_deferred(d, (char *) lfirst(nc)))
					unit_add_rel(d, (char *) lfirst(nc), t->lockmode);
		}
		foreach(lc, d->batch_locals)
		{
			LrLocalTxn *l = (LrLocalTxn *) lfirst(lc);
			ListCell   *nc;

			foreach(nc, l->rel_names)
				if (!name_is_deferred(d, (char *) lfirst(nc)))
					unit_add_rel(d, (char *) lfirst(nc), l->lockmode);
		}
	}

	/* the readers may not emit a table that is still being copied */
	if (d->defer_names != NIL)
		d->unit_spec.skip_relids = relid_set(d, d->defer_names);
	u->transport_private = &d->unit_spec;

	MemoryContextSwitchTo(oldcxt);
	return true;
}

/*
 * Let a held transaction go: the next DRAIN tells the reader, which is what
 * frees its buffer and lets its slot feedback move.  A distributed
 * transaction is named by its gxid, a utility write by (segment, position).
 */
static void
deferred_release(LrDriver *d, LrDeferredTxn *dt)
{
	if (dt->gxid != 0)
	{
		int64	   *g = palloc(sizeof(int64));

		*g = dt->gxid;
		d->release_gxids = lappend(d->release_gxids, g);
	}
	else
	{
		LrLocalTxn *l = palloc0(sizeof(LrLocalTxn));

		l->seg = dt->seg;
		l->seg_lsn = dt->event_lsn;
		d->release_locals = lappend(d->release_locals, l);
	}
}

/*
 * A catch-up unit: up to gp_lr_batch_max_txns of the transactions held for
 * a table that has just finished copying, in commit order, replayed for
 * that table alone.  The ones the copy already contains are not replayed at
 * all -- they committed before its cut point, so their rows are in it --
 * and are simply let go.
 *
 * Returns false when nothing is left to replay.
 */
static bool
unit_build_catchup(LrDriver *d)
{
	LogicalRepUnit *u = &d->unit;
	List	   *batch = NIL;
	ListCell   *lc;
	int			lockmode = RowExclusiveLock;
	int			n = 0;
	MemoryContext oldcxt = MemoryContextSwitchTo(d->cxt);

	d->unit_dropped = 0;
	while (batch == NIL && d->deferred_txns != NIL)
	{
		List	   *drop = NIL;

		foreach(lc, d->deferred_txns)
		{
			LrDeferredTxn *dt = (LrDeferredTxn *) lfirst(lc);

			if (list_length(batch) + list_length(drop) >= gp_lr_batch_max_txns)
				break;
			if (dt->gxid != 0
				? gxid_covered_by_add_sync(d, dt->gxid)
				: local_covered_by_add_sync(d, dt->seg, dt->xid))
			{
				drop = lappend(drop, dt);
				continue;
			}
			batch = lappend(batch, dt);
			lockmode = Max(lockmode, dt->lockmode);
		}

		foreach(lc, drop)
		{
			LrDeferredTxn *dt = (LrDeferredTxn *) lfirst(lc);

			deferred_release(d, dt);
			d->deferred_txns = list_delete_ptr(d->deferred_txns, dt);
			d->stats->txns_catchup_dropped++;
			d->unit_dropped++;
		}
		list_free(drop);
	}

	if (batch == NIL)
	{
		MemoryContextSwitchTo(oldcxt);
		return false;
	}
	d->unit_batch = batch;

	MemoryContextSwitchTo(d->batchcxt);
	memset(u, 0, sizeof(*u));
	memset(&d->unit_spec, 0, sizeof(d->unit_spec));
	u->ntxns = list_length(batch);
	u->txns = palloc0(sizeof(AssembledTxn) * u->ntxns);
	foreach(lc, batch)
	{
		LrDeferredTxn *dt = (LrDeferredTxn *) lfirst(lc);
		AssembledTxn *at = &u->txns[n++];
		int			np = 0;

		at->gxid = (uint64) dt->gxid;
		at->commit_order_pos = (XLogRecPtr) dt->event_lsn;

		/* a utility write is one segment's transaction and nothing else */
		if (dt->gxid == 0)
		{
			at->nparticipants = 1;
			at->participant_segidx = palloc(sizeof(int));
			at->participant_pos = palloc(sizeof(XLogRecPtr));
			at->participant_segidx[0] = dt->seg;
			at->participant_pos[0] = (XLogRecPtr) dt->event_lsn;
			u->commit_order_pos = at->commit_order_pos;
			continue;
		}

		at->participant_segidx = palloc(sizeof(int) * d->nsrcsegs);
		at->participant_pos = palloc(sizeof(XLogRecPtr) * d->nsrcsegs);
		for (int seg = 0; seg < d->nsrcsegs; seg++)
		{
			if (dt->seg_lsn[seg] < 0)
				continue;
			at->participant_segidx[np] = seg;
			at->participant_pos[np] = (XLogRecPtr) dt->seg_lsn[seg];
			np++;
		}
		at->nparticipants = np;
		u->commit_order_pos = at->commit_order_pos;
	}

	/* all the tables of the job, at the strongest lock the held ones need */
	if (d->caps.needs_relid_enumeration_for_lock)
	{
		u->rels = palloc(sizeof(LogicalRepRelLock) *
						 Max(list_length(d->sync_names), 1));
		foreach(lc, d->sync_names)
			unit_add_rel(d, (char *) lfirst(lc), lockmode);
	}

	d->unit_spec.only_relids = relid_set(d, d->sync_names);
	d->unit_spec.catchup = true;
	u->transport_private = &d->unit_spec;

	MemoryContextSwitchTo(oldcxt);
	return true;
}

/* ------------------------------------------------------------------------
 * LogicalRepAssembly methods
 * ------------------------------------------------------------------------
 */

#define LR_DRIVER(self)	((LrDriver *) (self)->private_data)

/* seg_pos / seg_flush / the coordinator position, as the target has them */
static void
assembly_take_progress(LrDriver *d, const ProgressPos *from)
{
	d->seg_pos = palloc0(sizeof(int64) * d->nsrcsegs);
	d->seg_flush = palloc0(sizeof(int64) * d->nsrcsegs);
	d->coord_committed = (int64) from->commit_order_pos;
	for (int seg = 0; seg < d->nsrcsegs && seg < from->nsegs; seg++)
	{
		d->seg_pos[seg] = (int64) from->seg_pos[seg];
		d->seg_flush[seg] = (int64) from->seg_flush[seg];
	}
}

static void
assembly_restart(LogicalRepAssembly *self, const ProgressPos *from)
{
	LrDriver   *d = LR_DRIVER(self);

	assembly_take_progress(d, from);
	driver_check_pending(d, true);
	driver_connect_readers(d);
	driver_connect_source(d);
}

static void
assembly_startup(LogicalRepAssembly *self, const SinkCaps *caps,
				 const ProgressPos *from)
{
	LrDriver   *d = LR_DRIVER(self);

	d->caps = *caps;

	/*
	 * How many source segments there are decides the size of every
	 * per-stream array, so it has to be known before the checkpoint is read
	 * into them.
	 */
	driver_load_source_segments(d);
	assembly_take_progress(d, from);
	driver_check_pending(d, true);
	driver_connect_readers(d);
	driver_connect_source(d);

	elog(LOG, "mpp apply worker: subscription %u started, %d target segments, %d source segments, coordinator position %X/%X",
		 d->cfg->subid, d->tr->nsegs, d->nsrcsegs,
		 LSN_FORMAT_ARGS((XLogRecPtr) d->coord_committed));
}

static void
assembly_shutdown(LogicalRepAssembly *self)
{
	LrDriver   *d = LR_DRIVER(self);
	LrWorkerStats *stats = d->stats;

	if (d->sync)
	{
		LrSyncAddAbort(d->sync);
		d->sync = NULL;
	}
	driver_disconnect_source(d);

	/*
	 * Anything still waiting has not been applied; say which, or a lost
	 * transaction is indistinguishable from one that was never sent.
	 */
	if (hash_get_num_entries(d->gxids) > 0)
	{
		HASH_SEQ_STATUS seq;
		LrGxidTxn  *t;
		StringInfoData buf;

		initStringInfo(&buf);
		hash_seq_init(&seq, d->gxids);
		while ((t = (LrGxidTxn *) hash_seq_search(&seq)) != NULL)
			appendStringInfo(&buf, "%s" INT64_FORMAT "(event=%s ready=%d/%d)",
							 buf.len ? ", " : "", t->gxid,
							 t->has_event ? "y" : "n", t->nready,
							 t->nsegs_expected);
		elog(LOG, "mpp apply worker: subscription %u still had %ld unapplied transaction(s): %s",
			 d->cfg->subid, hash_get_num_entries(d->gxids), buf.data);
		pfree(buf.data);
	}

	elog(LOG, "mpp apply worker: subscription %u stopped: " INT64_FORMAT " events, " INT64_FORMAT " batches, " INT64_FORMAT " gxid txns, " INT64_FORMAT " local txns, " INT64_FORMAT " deduped, " INT64_FORMAT " no-op, " INT64_FORMAT " caught up, " INT64_FORMAT " recoveries",
		 d->cfg->subid, stats->events, stats->batches, stats->txns_applied,
		 stats->local_applied, stats->events_deduped, stats->events_noop,
		 stats->txns_catchup, stats->recoveries);
}

/*
 * Read the sources.  The wait is what is left of this batch's latency
 * budget, so a batch closes on time even when the stream is quiet.
 */
static bool
assembly_poll(LogicalRepAssembly *self, int timeout_ms)
{
	LrDriver   *d = LR_DRIVER(self);
	int			timeout = gp_lr_batch_max_latency_ms;

	if (d->batch_gxids != NIL || d->batch_locals != NIL)
	{
		long		age = TimestampDifferenceMilliseconds(d->batch_started,
														  GetCurrentTimestamp());

		timeout = Max(0, gp_lr_batch_max_latency_ms - (int) age);
	}
	timeout = Min(timeout, timeout_ms);

	if (!driver_pump_source(d, timeout))
		return false;

	/* idle: ask the readers for local transactions and their positions */
	if (TimestampDifferenceExceeds(d->last_drain, GetCurrentTimestamp(),
								   gp_lr_idle_drain_ms))
	{
		driver_drain(d, 0, 0, NULL, 0);
		driver_collect_ready(d);
	}
	driver_publish(d);
	return true;
}

/*
 * While the held transactions of a finished copy are being replayed, they
 * are the only units handed out: nothing else may run between them, or the
 * set could grow while it is drained.
 */
static bool
assembly_next_unit(LogicalRepAssembly *self, LogicalRepUnit *out)
{
	LrDriver   *d = LR_DRIVER(self);

	if (d->catchup_mode)
	{
		if (!unit_build_catchup(d))
			return false;
	}
	else
	{
		if (!driver_batch_ready(d))
			return false;
		unit_build_from_batch(d);
	}

	*out = d->unit;
	return true;
}

/*
 * Where the subscription stands once this unit is committed.
 *
 * seg_flush is the oldest position the readers may still be asked to replay
 * from: the earliest of everything ready but not in this unit, everything
 * held for a table still being copied, and the position already reached.
 * It never moves backwards.
 *
 * A catch-up unit reports nothing (nsegs = 0): its transactions were
 * counted when they were first applied, and the positions they hold down
 * are released by the next ordinary unit.
 */
static void
assembly_unit_progress(LogicalRepAssembly *self, const LogicalRepUnit *unit,
					   ProgressPos *out)
{
	LrDriver   *d = LR_DRIVER(self);
	MemoryContext oldcxt;
	HASH_SEQ_STATUS seq;
	LrGxidTxn  *t;
	ListCell   *lc;
	int			napplied = 0;
	int			nsegs = d->nsrcsegs;

	memset(out, 0, sizeof(*out));
	if (d->unit_spec.catchup)
		return;

	oldcxt = MemoryContextSwitchTo(d->batchcxt);

	d->unit_flush = palloc(sizeof(int64) * nsegs);
	for (int seg = 0; seg < nsegs; seg++)
		d->unit_flush[seg] = d->seg_pos[seg];

	hash_seq_init(&seq, d->gxids);
	while ((t = (LrGxidTxn *) hash_seq_search(&seq)) != NULL)
	{
		if (t->in_batch)
			continue;
		for (int seg = 0; seg < nsegs; seg++)
			if (t->parts[seg].ready && t->parts[seg].seg_lsn < d->unit_flush[seg])
				d->unit_flush[seg] = t->parts[seg].seg_lsn;
	}
	foreach(lc, d->locals)
	{
		LrLocalTxn *l = (LrLocalTxn *) lfirst(lc);

		if (l->seg_lsn < d->unit_flush[l->seg])
			d->unit_flush[l->seg] = l->seg_lsn;
	}
	foreach(lc, d->deferred_txns)
	{
		LrDeferredTxn *dt = (LrDeferredTxn *) lfirst(lc);

		if (dt->gxid == 0)
		{
			/* a utility write holds back the one segment it belongs to */
			if (dt->seg < nsegs && dt->event_lsn < d->unit_flush[dt->seg])
				d->unit_flush[dt->seg] = dt->event_lsn;
			continue;
		}
		for (int seg = 0; seg < nsegs; seg++)
			if (dt->seg_lsn[seg] >= 0 && dt->seg_lsn[seg] < d->unit_flush[seg])
				d->unit_flush[seg] = dt->seg_lsn[seg];
	}
	for (int seg = 0; seg < nsegs; seg++)
		if (d->unit_flush[seg] < d->seg_flush[seg])
			d->unit_flush[seg] = d->seg_flush[seg];

	out->commit_order_pos = (XLogRecPtr) d->unit_coord_lsn;
	out->nsegs = nsegs;
	out->seg_pos = palloc(sizeof(XLogRecPtr) * nsegs);
	out->seg_flush = palloc(sizeof(XLogRecPtr) * nsegs);
	for (int seg = 0; seg < nsegs; seg++)
	{
		out->seg_pos[seg] = (XLogRecPtr) d->seg_pos[seg];
		out->seg_flush[seg] = (XLogRecPtr) d->unit_flush[seg];
	}

	/* the deduplication triples of the distributed transactions (13.1) */
	for (int i = 0; i < unit->ntxns; i++)
		if (unit->txns[i].gxid != 0)
			napplied += unit->txns[i].nparticipants;
	out->applied_segidx = palloc(sizeof(int) * Max(napplied, 1));
	out->applied_gxid = palloc(sizeof(uint64) * Max(napplied, 1));
	out->applied_pos = palloc(sizeof(XLogRecPtr) * Max(napplied, 1));
	for (int i = 0; i < unit->ntxns; i++)
	{
		const AssembledTxn *at = &unit->txns[i];

		if (at->gxid == 0)
			continue;
		for (int p = 0; p < at->nparticipants; p++)
		{
			out->applied_segidx[out->napplied] = at->participant_segidx[p];
			out->applied_gxid[out->napplied] = at->gxid;
			out->applied_pos[out->napplied] = at->participant_pos[p];
			out->napplied++;
		}
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * The unit is committed.  Its transactions may be let go -- the next DRAIN
 * tells the readers, which is also when they may move their slot feedback
 * -- unless they also touched a table that is still being copied, in which
 * case the reader keeps the buffer and the transaction waits for the
 * catch-up.
 */
static void
assembly_advance(LogicalRepAssembly *self, const LogicalRepUnit *unit)
{
	LrDriver   *d = LR_DRIVER(self);
	ListCell   *lc;
	MemoryContext oldcxt = MemoryContextSwitchTo(d->cxt);

	if (d->unit_spec.catchup)
	{
		d->stuck_errors = 0;
		d->stats->batches++;
		d->stats->txns_catchup += list_length(d->unit_batch);

		foreach(lc, d->unit_batch)
		{
			LrDeferredTxn *dt = (LrDeferredTxn *) lfirst(lc);

			deferred_release(d, dt);
			d->deferred_txns = list_delete_ptr(d->deferred_txns, dt);
		}
		elog(LOG, "mpp apply worker: subscription %u: caught up %d transaction(s), dropped %d already copied, %d left",
			 d->cfg->subid, list_length(d->unit_batch), d->unit_dropped,
			 list_length(d->deferred_txns));
		list_free(d->unit_batch);
		d->unit_batch = NIL;

		/* the readers give the buffers back on the next drain */
		driver_drain(d, 0, 0, NULL, 0);
		d->rep.last_commit_time = GetCurrentTimestamp();
		driver_publish(d);
		MemoryContextSwitchTo(oldcxt);
		MemoryContextReset(d->batchcxt);
		return;
	}

	d->stuck_errors = 0;		/* the run is making progress again */
	d->coord_committed = d->unit_coord_lsn;
	memcpy(d->seg_flush, d->unit_flush, sizeof(int64) * d->nsrcsegs);
	d->stats->batches++;
	d->stats->txns_applied += list_length(d->batch_gxids);
	d->stats->local_applied += list_length(d->batch_locals);

	foreach(lc, d->batch_gxids)
	{
		LrGxidTxn  *t = (LrGxidTxn *) lfirst(lc);

		if (txn_is_deferred(d, t->rel_names))
		{
			LrDeferredTxn *dt = palloc0(sizeof(LrDeferredTxn));

			dt->gxid = t->gxid;
			dt->event_lsn = t->event_lsn;
			dt->lockmode = t->lockmode;
			dt->seg_lsn = palloc(sizeof(int64) * d->nsrcsegs);
			for (int seg = 0; seg < d->nsrcsegs; seg++)
				dt->seg_lsn[seg] = t->parts[seg].ready ? t->parts[seg].seg_lsn : -1;
			d->deferred_txns = lappend(d->deferred_txns, dt);
		}
		else
		{
			int64	   *g = palloc(sizeof(int64));

			*g = t->gxid;
			d->release_gxids = lappend(d->release_gxids, g);
		}
		registry_remove(d, t);
	}
	foreach(lc, d->batch_locals)
	{
		LrLocalTxn *src = (LrLocalTxn *) lfirst(lc);

		if (txn_is_deferred(d, src->rel_names))
		{
			LrDeferredTxn *dt = palloc0(sizeof(LrDeferredTxn));

			dt->gxid = 0;		/* a utility write; seg + event_lsn name it */
			dt->xid = src->xid;
			dt->seg = src->seg;
			dt->event_lsn = src->seg_lsn;
			dt->lockmode = src->lockmode;
			d->deferred_txns = lappend(d->deferred_txns, dt);
		}
		else
		{
			LrLocalTxn *l = palloc0(sizeof(LrLocalTxn));

			l->seg = src->seg;
			l->seg_lsn = src->seg_lsn;
			d->release_locals = lappend(d->release_locals, l);
		}
	}

	elog(LOG, "mpp apply worker: batch " INT64_FORMAT " committed: %d gxid txns, %d local txns, %d tables, coordinator position %X/%X",
		 d->stats->batches, list_length(d->batch_gxids),
		 list_length(d->batch_locals), unit->nrels,
		 LSN_FORMAT_ARGS((XLogRecPtr) d->unit_coord_lsn));

	d->batch_gxids = NIL;
	d->batch_locals = NIL;
	d->batch_bytes = 0;
	d->rep.last_commit_time = GetCurrentTimestamp();
	driver_publish(d);

	MemoryContextSwitchTo(oldcxt);
	MemoryContextReset(d->batchcxt);

	driver_send_feedback(d);
}

static bool
assembly_housekeeping(LogicalRepAssembly *self)
{
	LrDriver   *d = LR_DRIVER(self);
	bool		more;

	/* tables added to the subscription, and the copy they need */
	more = driver_sync_progress(d);

	if (TimestampDifferenceExceeds(d->last_feedback, GetCurrentTimestamp(),
								   LR_FEEDBACK_MS))
		driver_send_feedback(d);

	return more;
}

/*
 * Copy what an operator may want to see into the shared-memory slot (work
 * item O2).  Called once per pass of the loop and after every commit, so an
 * idle subscription still shows its positions moving and a busy one shows
 * its batches; the per-segment buffering comes from the readers' own status
 * reports as they arrive.
 */
static void
driver_publish(LrDriver *d)
{
	LrWorkerReport *r = &d->rep;
	int			n = Min(d->nsrcsegs, LR_STAT_MAX_SEGS);
	int			i;

	r->phase = (d->sync != NULL) ? LR_PHASE_TABLE_SYNC : LR_PHASE_STREAMING;
	r->received_lsn = d->coord_pos;
	r->applied_lsn = d->coord_committed;
	r->nsrcsegs = n;
	for (i = 0; i < n; i++)
	{
		r->seg_pos[i] = d->seg_pos[i];
		r->seg_flush[i] = d->seg_flush[i];
	}
	r->awaited_txns = d->gxids ? hash_get_num_entries(d->gxids) : 0;
	r->deferred_txns = list_length(d->deferred_txns);
	r->batch_txns = list_length(d->batch_gxids) + list_length(d->batch_locals);
	r->batch_bytes = d->batch_bytes;
	r->stuck_errors = d->stuck_errors;
	r->stats = *d->stats;
	LrStatPublish(r);
}

/*
 * Can this error pass on its own?  A lost connection, a reset gang, a
 * deadlock victim, a cancelled statement: replaying from the last committed
 * position is exactly right for those.  Everything else -- a relation the
 * target does not have, a replica identity that does not carry what apply
 * needs, a constraint the stream violates -- comes back on every replay,
 * because the replay is the same.
 */
static bool
error_is_transient(int sqlerrcode)
{
	switch (ERRCODE_TO_CATEGORY(sqlerrcode))
	{
		case ERRCODE_CONNECTION_EXCEPTION:
		case ERRCODE_TRANSACTION_ROLLBACK:	/* deadlock, serialisation */
		case ERRCODE_INSUFFICIENT_RESOURCES:
		case ERRCODE_OPERATOR_INTERVENTION:	/* cancel, terminate, shutdown */
			return true;
	}
	return false;
}

/*
 * Decide what to do about an error the loop caught.
 *
 * Retrying is the right answer for anything that can pass; for anything else
 * it only hides the reason in a log that repeats every second or so, and --
 * worse -- nothing else gets applied meanwhile, so a table added to the
 * subscription afterwards is never even copied.  After
 * gp_lr_apply_max_retries of those the subscription disables itself and says
 * why, which is the state an operator can act on.
 */
static bool
assembly_handle_error(LogicalRepAssembly *self, int sqlerrcode, const char *message)
{
	LrDriver   *d = LR_DRIVER(self);

	if (error_is_transient(sqlerrcode))
	{
		d->stuck_errors = 0;
		return true;
	}

	d->stuck_errors++;
	elog(DEBUG1, "mpp apply worker: error [%s] that does not pass on its own, %d in a row",
		 unpack_sql_state(sqlerrcode), d->stuck_errors);
	if (gp_lr_apply_max_retries <= 0 ||
		d->stuck_errors < gp_lr_apply_max_retries ||
		gp_lr_apply_error_action == LR_APPLY_ERROR_RETRY)
	{
		if (d->stuck_errors == gp_lr_apply_max_retries &&
			gp_lr_apply_error_action == LR_APPLY_ERROR_RETRY)
			ereport(WARNING,
					(errmsg("mpp apply worker: subscription %u has failed %d times without applying anything: %s",
							d->cfg->subid, d->stuck_errors, message),
					 errhint("The error is not one that passes on its own. Set mpp_logicalrep.apply_error_action=disable to stop the subscription instead of retrying.")));
		return true;
	}

	lr_disable_subscription(d->tr, d->cfg->subid, message);

	ereport(WARNING,
			(errmsg("mpp apply worker: subscription %u disabled after %d attempts: %s",
					d->cfg->subid, d->stuck_errors, message),
			 errdetail("The error came back on every replay, so nothing of the subscription was being applied."),
			 errhint("Fix what the message names and re-enable the subscription, or set mpp_logicalrep.apply_error_action=retry to keep trying.")));
	return false;
}

/*
 * Throw away everything not committed.  What the readers buffered is not
 * lost: the flush position was never allowed to pass a transaction that is
 * still needed, so a reconnect replays from there and the applied triples
 * drop what is already in.
 */
static void
assembly_reset(LogicalRepAssembly *self)
{
	LrDriver   *d = LR_DRIVER(self);
	HASH_SEQ_STATUS seq;
	LrGxidTxn  *t;

	d->stats->recoveries++;

	/*
	 * An incremental synchronisation in flight is abandoned: its tables stay
	 * pending in gp_subscription_rel and the next attempt redoes them under
	 * a fresh cut point.
	 */
	if (d->sync)
	{
		LrSyncAddAbort(d->sync);
		d->sync = NULL;
		d->sync_names = NIL;
		d->sync_ds_xmax = 0;
		d->sync_nds_inprogress = 0;
		d->sync_nlocal = 0;
		d->sync_local = NULL;
	}
	d->catchup_mode = false;
	d->deferred_txns = NIL;
	d->unit_batch = NIL;

	hash_seq_init(&seq, d->gxids);
	while ((t = (LrGxidTxn *) hash_seq_search(&seq)) != NULL)
		registry_remove(d, t);
	d->locals = NIL;
	d->batch_gxids = NIL;
	d->batch_locals = NIL;
	d->batch_bytes = 0;
	MemoryContextReset(d->batchcxt);
	d->release_gxids = NIL;
	d->release_locals = NIL;

	driver_disconnect_source(d);
}

/*
 * The worker's wait: its SIGTERM handler sets the latch, so a stop asked for
 * while the run is backing off ends the wait rather than waiting it out.
 */
static void
assembly_wait(LogicalRepAssembly *self, int ms)
{
	(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 ms, WAIT_EVENT_LOGICAL_APPLY_MAIN);
	ResetLatch(MyLatch);
}

static bool
assembly_should_stop(LogicalRepAssembly *self)
{
	LrDriver   *d = LR_DRIVER(self);

	/*
	 * Process catalog invalidations so that a subscription change (DISABLE,
	 * ALTER, DROP) reaches the syscache callback the worker registered; this
	 * process otherwise only does libpq I/O and would never notice.
	 * Harmless in the SQL test entry, which registers no such callback.
	 */
	AcceptInvalidationMessages();

	/*
	 * And a configuration reload, for the same reason: nothing else in this
	 * loop would ever look, so a changed gp_lr_* setting would only take
	 * effect when the subscription is restarted.  Between units, with no
	 * target transaction open, is the safe place for it.
	 */
	if (ConfigReloadPending)
	{
		ConfigReloadPending = false;
		ProcessConfigFile(PGC_SIGHUP);
	}

	if (d->cfg->should_stop && d->cfg->should_stop(d->cfg->stop_arg))
		return true;
	if (d->cfg->max_batches > 0 && d->stats->batches >= d->cfg->max_batches)
		return true;
	if (d->cfg->max_seconds > 0 &&
		TimestampDifferenceExceeds(d->started, GetCurrentTimestamp(),
								   d->cfg->max_seconds * 1000))
		return true;
	return false;
}

LogicalRepAssembly *
LrMakeCbdbAssembly(LrCbdbTransport *tr, LrWorkerConfig *cfg, LrWorkerStats *stats)
{
	LogicalRepAssembly *assembly = palloc0(sizeof(LogicalRepAssembly));
	LrDriver   *d = palloc0(sizeof(LrDriver));
	HASHCTL		ctl;

	d->tr = tr;
	d->cfg = cfg;
	d->mycfg = cfg;
	d->stats = stats;
	d->cxt = CurrentMemoryContext;
	d->batchcxt = AllocSetContextCreate(d->cxt, "LrDriver batch",
										ALLOCSET_DEFAULT_SIZES);
	d->started = GetCurrentTimestamp();

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int64);
	ctl.entrysize = sizeof(LrGxidTxn);
	ctl.hcxt = d->cxt;
	d->gxids = hash_create("LrDriver gxids", 1024, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	assembly->startup = assembly_startup;
	assembly->shutdown = assembly_shutdown;
	assembly->poll = assembly_poll;
	assembly->next_unit = assembly_next_unit;
	assembly->next_change = NULL;	/* the sink takes its rows out of band */
	assembly->unit_progress = assembly_unit_progress;
	assembly->advance = assembly_advance;
	assembly->housekeeping = assembly_housekeeping;
	assembly->handle_error = assembly_handle_error;
	assembly->reset = assembly_reset;
	assembly->restart = assembly_restart;
	assembly->should_stop = assembly_should_stop;
	assembly->wait = assembly_wait;
	assembly->private_data = d;

	return assembly;
}

/* ------------------------------------------------------------------------
 * Background worker entry point
 * ------------------------------------------------------------------------
 *
 * The extension's launcher (mpp_logicalrep.c) starts one of these per
 * enabled MPP subscription, on the coordinator only.  It does not use the
 * server's own apply-worker machinery: a subscription this extension owns is
 * left disabled in pg_subscription precisely so that the server's launcher
 * ignores it, so there is no LogicalRepWorker slot to attach to and no
 * MySubscription to inherit.  What the driver needs of that machinery is
 * little: a database connection as the subscription owner, the replica
 * session role, and the subscription row.
 *
 * The driver process itself touches no user table; it drives a normal
 * coordinator session through libpq.
 */
static volatile sig_atomic_t mpp_worker_stop = false;

static void
mpp_worker_sigterm(SIGNAL_ARGS)
{
	mpp_worker_stop = true;
	SetLatch(MyLatch);
}

/*
 * Has this worker been told to stop?  Cheap enough to ask while waiting for
 * a socket, which is the point: it is what makes the libpq waits above give
 * up on a SIGTERM.  Always false in the SQL test entry, which runs in an
 * ordinary backend where CHECK_FOR_INTERRUPTS does the same job.
 */
bool
LrWorkerStopRequested(void)
{
	return mpp_worker_stop;
}

static bool
mpp_worker_should_stop(void *arg)
{
	return mpp_worker_stop || !LrSubscriptionEnabled(*(Oid *) arg);
}

/*
 * The subscription row changed (ALTER, DROP): stop the loop.  The launcher
 * restarts the worker only if the subscription is still enabled in the
 * extension's registry, so a spurious wake just causes a quick restart.
 */
static void
mpp_worker_subscription_cb(Datum arg, int cacheid, uint32 hashvalue)
{
	/*
	 * Any subscription DDL invalidates pg_subscription, and a worker that
	 * stopped for someone else's would be restarted for nothing (the server's
	 * own worker.c does exactly that).  hashvalue 0 means "all of them".
	 */
	if (hashvalue != 0 && hashvalue != DatumGetUInt32(arg))
		return;
	mpp_worker_stop = true;
	SetLatch(MyLatch);
}

void
MppApplyWorkerMain(Datum main_arg)
{
	LrWorkerArg arg;
	LrWorkerConfig cfg;
	LrWorkerStats stats;
	Subscription *sub;
	char	   *dbname;
	char	   *username;
	Oid			subid;

	memcpy(&arg, MyBgworkerEntry->bgw_extra, sizeof(arg));
	subid = arg.subid;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, mpp_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	/*
	 * Cloudberry builds the walreceiver functions into the backend rather
	 * than loading them as a library; the stock apply worker does the same.
	 */
	if (!WalReceiverFunctions)
		libpqwalreceiver_PG_init();

	/* apply runs as the replica session replication role */
	SetConfigOption("session_replication_role", "replica",
					PGC_SUSET, PGC_S_OVERRIDE);

	BackgroundWorkerInitializeConnectionByOid(arg.dbid, arg.userid, 0);

	/* always-secure search path, as the stock apply worker uses */
	SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * Claim the shared slot the launcher uses to see that this subscription
	 * has a worker, and confirm it is ours and enabled; this does not return
	 * if it is not.
	 */
	LrWorkerClaim(subid, arg.dbid);

	memset(&cfg, 0, sizeof(cfg));
	cfg.subid = subid;
	cfg.token = (int64) MyProcPid;

	/*
	 * Load the subscription, and build the conninfo of the driven session: a
	 * normal QD backend of this same coordinator, reached over the local Unix
	 * socket (no host), which is the trust path and avoids depending on
	 * pg_hba for loopback TCP.  Both go into a long-lived context inside a
	 * transaction (the lookups need catalog access) so they outlive it.
	 */
	StartTransactionCommand();
	{
		MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);

		sub = GetSubscription(subid, true);
		if (sub == NULL)
		{
			MemoryContextSwitchTo(oldctx);
			CommitTransactionCommand();
			ereport(LOG,
					(errmsg("subscription %u was removed during startup; MPP apply worker exiting",
							subid)));
			proc_exit(0);
		}
		dbname = get_database_name(arg.dbid);
		username = GetUserNameFromId(arg.userid, false);
		cfg.target_conninfo = psprintf("port=%d dbname=%s user=%s",
									   PostPortNumber, dbname, username);

		/*
		 * Stop cleanly when this subscription is altered or dropped -- and
		 * only this one.  The callback gets the syscache hash value of our
		 * own row, which computing here, inside a transaction, is safe: it
		 * may have to open pg_subscription the first time.
		 */
		CacheRegisterSyscacheCallback(SUBSCRIPTIONOID, mpp_worker_subscription_cb,
									  UInt32GetDatum(GetSysCacheHashValue1(SUBSCRIPTIONOID,
																		   ObjectIdGetDatum(subid))));
		MemoryContextSwitchTo(oldctx);
	}
	CommitTransactionCommand();

	if (sub->slotname == NULL)
	{
		ereport(LOG,
				(errmsg("MPP subscription \"%s\" has no slot name; apply worker exiting",
						sub->name)));
		proc_exit(0);
	}

	/* the subscription's own synchronous_commit setting */
	SetConfigOption("synchronous_commit", sub->synccommit,
					PGC_BACKEND, PGC_S_OVERRIDE);

	cfg.subname = sub->name;
	cfg.source_conninfo = sub->conninfo;
	cfg.coord_slot = sub->slotname;
	cfg.seg_slot = sub->slotname;
	cfg.publications = sub->publications;
	cfg.should_stop = mpp_worker_should_stop;
	cfg.stop_arg = &subid;
	cfg.initial_sync = true;

	ereport(LOG,
			(errmsg("MPP apply worker for subscription \"%s\" started (slot \"%s\")",
					sub->name, sub->slotname)));

	LrApplyWorkerRun(&cfg, &stats);

	ereport(LOG,
			(errmsg("MPP apply worker for subscription \"%s\" exiting",
					sub->name)));
	proc_exit(0);
}
