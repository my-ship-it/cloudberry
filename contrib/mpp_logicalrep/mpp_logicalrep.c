/*-------------------------------------------------------------------------
 *
 * mpp_logicalrep.c
 *	  Extension entry point: settings, the CREATE SUBSCRIPTION option, and
 *	  the launcher that runs one apply driver per MPP subscription.
 *
 * Everything the apply path needs of the server is either a normal backend
 * function or something the server offers every extension.  The three
 * pieces that are not obviously either are here:
 *
 * 1. The subscription DDL.  A loaded library is what says a subscription is
 *	  ours -- there is no option to write, because on Cloudberry there is no
 *	  second kind that works -- and ProcessUtility_hook sees the parsed
 *	  statement before the server acts on it.  It adds connect = false, which
 *	  leaves the catalog row disabled, and a disabled subscription is one the
 *	  server's own launcher never starts a worker for; a stock apply worker
 *	  here would write the rows on the coordinator, where no user table has
 *	  any.  The options that say how to do the things the module replaces
 *	  (connect, create_slot) are therefore refused rather than obeyed, and
 *	  the ones that are about the subscription rather than the mechanism
 *	  (enabled, copy_data, unsupported_tables) are taken out of the statement
 *	  and acted on here.
 *	  There is no option to ask for a subscription of the server's own kind:
 *	  where this module is loaded, every subscription is applied by it.
 *
 * 2. The launcher.  A background worker registered at postmaster start,
 *	  which polls this extension's own registry (mpp_subscription) on the
 *	  coordinator and starts a dynamic worker per enabled subscription.
 *
 * 3. The custom scan methods of the apply plans (mpp_apply_plan.c).  Any
 *	  backend may have to deserialize such a plan on dispatch, so they must
 *	  be registered everywhere, which is what _PG_init in a preloaded
 *	  library gives: the postmaster registers them once and every backend
 *	  inherits the registration.
 *
 * Because of 2 and 3 the library must be in shared_preload_libraries.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_logicalrep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_subscription_rel.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "libpq/pqsignal.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/pg_lsn.h"
#include "utils/tuplestore.h"
#include "replication/walreceiver.h"
#include "miscadmin.h"

#include "mpp_apply.h"
#include "mpp_apply_worker.h"
#include "mpp_reader.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* how often the launcher looks for subscriptions to start or stop */
static int	launcher_naptime_ms = 5000;

static ProcessUtility_hook_type prev_ProcessUtility = NULL;

static const struct config_enum_entry slot_loss_action_options[] = {
	{"reinit", LR_SLOT_LOSS_REINIT, false},
	{"disable", LR_SLOT_LOSS_DISABLE, false},
	{NULL, 0, false}
};

static const struct config_enum_entry apply_error_action_options[] = {
	{"disable", LR_APPLY_ERROR_DISABLE, false},
	{"retry", LR_APPLY_ERROR_RETRY, false},
	{NULL, 0, false}
};

/* ------------------------------------------------------------------------
 * Settings
 * ------------------------------------------------------------------------
 */
/*
 * The address ends up on the command line of the copy helper, which a shell
 * runs, so nothing but a host address may get in.  Checking it here means a
 * bad value is refused by ALTER SYSTEM rather than by a copy an hour later.
 */
static bool
check_copy_bind_address(char **newval, void **extra, GucSource source)
{
	for (const char *c = *newval; c && *c; c++)
		if (!((*c >= 'A' && *c <= 'Z') || (*c >= 'a' && *c <= 'z') ||
			  (*c >= '0' && *c <= '9') || *c == '.' || *c == ':' ||
			  *c == '-' || *c == '_'))
		{
			GUC_check_errdetail("It must be a host address: letters, digits and \".:-_\".");
			return false;
		}
	return true;
}

static void
define_gucs(void)
{
	DefineCustomIntVariable("mpp_logicalrep.batch_max_txns",
							"Transactions per batch before it is applied.",
							NULL,
							&gp_lr_batch_max_txns,
							64, 1, 100000,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.batch_max_bytes",
							"Buffered change bytes per batch before it is applied.",
							NULL,
							&gp_lr_batch_max_bytes,
							65536, 64, MAX_KILOBYTES,
							PGC_USERSET, GUC_UNIT_KB, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.batch_max_latency_ms",
							"Age of a batch before it is applied.",
							NULL,
							&gp_lr_batch_max_latency_ms,
							200, 0, 3600000,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.apply_max_retries",
							"How many times to retry an error that makes no progress.",
							"Counted per run and reset by every committed batch; 0 means never "
							"give up. What happens when the count is reached is "
							"mpp_logicalrep.apply_error_action.",
							&gp_lr_apply_max_retries,
							10, 0, INT_MAX,
							PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.event_wait_ms",
							"How long one drain waits for the transaction of a commit event.",
							NULL,
							&gp_lr_event_wait_ms,
							1000, 0, 3600000,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.idle_drain_ms",
							"Poll the readers this often while the source is idle.",
							NULL,
							&gp_lr_idle_drain_ms,
							2000, 100, 3600000,
							PGC_USERSET, GUC_UNIT_MS, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.reader_mem",
							"Memory a reader QE may use to buffer changes before spilling to disk.",
							"Per subscription.",
							&gp_lr_reader_mem,
							65536, 64, MAX_KILOBYTES,
							PGC_USERSET, GUC_UNIT_KB, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.max_concurrent_copies",
							"How many tables the initial synchronisation copies at once.",
							"Each one takes a session on either side and a port on every "
							"target segment, so the port range bounds it.  Two is what a "
							"three-segment cluster on one host was measured saturating at; "
							"raise it where there is capacity to use.",
							&gp_lr_max_concurrent_copies,
							2, 1, 16,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.hold_max",
							"How much one reader may buffer before the coordinator stops reading the source.",
							"Per source segment, memory and spill together.  0 disables the limit.",
							&gp_lr_hold_max,
							1048576, 0, MAX_KILOBYTES,
							PGC_USERSET, GUC_UNIT_KB, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.copy_port_lo",
							"Initial sync: low end of the TCP port range the copy helper listens on.",
							NULL,
							&gp_lr_copy_port_lo,
							42000, 1024, 65535,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.copy_port_hi",
							"Initial sync: high end of the TCP port range the copy helper listens on.",
							NULL,
							&gp_lr_copy_port_hi,
							42999, 1024, 65535,
							PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("mpp_logicalrep.copy_crc",
							 "Initial sync: checksum each copy-transport frame.",
							 NULL,
							 &gp_lr_copy_crc,
							 true,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomBoolVariable("mpp_logicalrep.direct_copy",
							 "Initial sync: load a table where it lands when both sides place its rows the same way.",
							 "Off makes every table go through a redistribution, as it did before the direct path existed.",
							 &gp_lr_direct_copy,
							 true,
							 PGC_USERSET, 0, NULL, NULL, NULL);

	DefineCustomStringVariable("mpp_logicalrep.copy_bind_address",
							   "Address the initial-sync copy transport listens on.",
							   "Empty means every interface. The socket has no authentication beyond the job name the sender opens with, so on a host with an interface the source cluster does not use, name the one it does.",
							   &gp_lr_copy_bind_address,
							   "",
							   PGC_SIGHUP, 0, check_copy_bind_address, NULL, NULL);

	DefineCustomEnumVariable("mpp_logicalrep.slot_loss_action",
							 "What an apply worker does when the source's replication slots are gone.",
							 "reinit: redo the initial synchronisation of the whole subscription. "
							 "disable: disable the subscription and leave it to an operator.",
							 &gp_lr_slot_loss_action,
							 LR_SLOT_LOSS_REINIT, slot_loss_action_options,
							 PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomEnumVariable("mpp_logicalrep.apply_error_action",
							 "What an apply worker does with an error that keeps coming back.",
							 "disable: disable the subscription and say why, after "
							 "mpp_logicalrep.apply_max_retries attempts. retry: keep retrying for "
							 "ever. Errors that can pass on their own (a lost connection, a reset "
							 "gang, a deadlock) are always retried.",
							 &gp_lr_apply_error_action,
							 LR_APPLY_ERROR_DISABLE, apply_error_action_options,
							 PGC_SIGHUP, 0, NULL, NULL, NULL);

	DefineCustomIntVariable("mpp_logicalrep.launcher_naptime_ms",
							"How often the launcher looks for subscriptions to start.",
							NULL,
							&launcher_naptime_ms,
							5000, 100, 3600000,
							PGC_SIGHUP, GUC_UNIT_MS, NULL, NULL, NULL);

	MarkGUCPrefixReserved("mpp_logicalrep");
}

/* ------------------------------------------------------------------------
 * Shared state
 * ------------------------------------------------------------------------
 *
 * The registry that says which subscriptions this extension owns is an
 * ordinary table, so it lives in one database -- while the launcher, like
 * the server's own, runs without a database connection and can only read
 * shared catalogs.  It therefore works the other way round: the launcher
 * proposes (any subscription shaped like one of ours: disabled, with a slot
 * name) and the worker disposes (it connects, reads the registry, and says
 * so here).  A verdict of "not ours" is remembered so the probe happens once
 * per subscription rather than once per nap.
 */
typedef struct LrWorkerSlot
{
	Oid			subid;			/* InvalidOid: free */
	Oid			dbid;
	int			pid;			/* 0: no worker running */
	bool		not_ours;		/* the worker found no registry row */
	LrWorkerReport rep;			/* what the worker last published (O2) */
} LrWorkerSlot;

typedef struct LrSharedState
{
	LWLock	   *lock;
	int			nslots;
	int			launcher_pid;	/* 0: no launcher; woken by SIGUSR1 */
	LrWorkerSlot slots[FLEXIBLE_ARRAY_MEMBER];
} LrSharedState;

static LrSharedState *lr_shared = NULL;
static LrWorkerSlot *my_slot = NULL; /* this worker's own, once attached */
static int	max_workers = 8;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static Size
lr_shmem_size(void)
{
	return MAXALIGN(offsetof(LrSharedState, slots) +
					sizeof(LrWorkerSlot) * max_workers);
}

static void
lr_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(lr_shmem_size());
	RequestNamedLWLockTranche("mpp_logicalrep", 1);
}

static void
lr_shmem_startup(void)
{
	bool		found;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	lr_shared = ShmemInitStruct("mpp_logicalrep", lr_shmem_size(), &found);
	if (!found)
	{
		memset(lr_shared, 0, lr_shmem_size());
		lr_shared->lock = &(GetNamedLWLockTranche("mpp_logicalrep"))->lock;
		lr_shared->nslots = max_workers;
	}
	LWLockRelease(AddinShmemInitLock);
}

/* the slot of this subscription, or NULL; caller holds the lock */
static LrWorkerSlot *
slot_find(Oid subid)
{
	int			i;

	for (i = 0; i < lr_shared->nslots; i++)
		if (lr_shared->slots[i].subid == subid)
			return &lr_shared->slots[i];
	return NULL;
}

/*
 * A worker announces itself.  Returns false if there is no room, in which
 * case the worker must exit: without a slot the launcher cannot tell that it
 * is running and would start a second one.
 */
static bool
lr_worker_attach(Oid subid, Oid dbid)
{
	LrWorkerSlot *slot;
	int			i;
	bool		ok = false;

	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	slot = slot_find(subid);
	if (slot == NULL)
	{
		LrWorkerSlot *idle = NULL;

		for (i = 0; i < lr_shared->nslots; i++)
		{
			if (!OidIsValid(lr_shared->slots[i].subid))
			{
				slot = &lr_shared->slots[i];
				break;
			}

			/*
			 * A slot is not given up when its worker goes, so that the
			 * monitoring view can still name a subscription that has stopped.
			 * That is worth having, but not worth a subscription that can
			 * never start: when every slot is spoken for, the ones whose
			 * worker is gone are taken back, least recently started first.
			 * The view falls back to the state tables for those.
			 */
			if (lr_shared->slots[i].pid == 0 &&
				(idle == NULL ||
				 lr_shared->slots[i].rep.start_time < idle->rep.start_time))
				idle = &lr_shared->slots[i];
		}
		if (slot == NULL && idle != NULL)
		{
			/*
			 * Taking one back is better than a subscription that can never
			 * start, but it is worth saying: it means max_workers is too
			 * small for the number of subscriptions this cluster runs, and
			 * the one evicted loses its row in the monitoring view until its
			 * own worker runs again.  Silence here would make a shortage look
			 * like a subscription that merely takes a while to come up.
			 */
			ereport(LOG,
					(errmsg("mpp_logicalrep: taking the worker slot of subscription %u for subscription %u",
							idle->subid, subid),
					 errhint("Raise \"mpp_logicalrep.max_workers\" above the number of subscriptions this cluster runs.")));
			slot = idle;
		}
		if (slot != NULL)
		{
			slot->subid = subid;
			slot->not_ours = false;
		}
	}
	if (slot != NULL)
	{
		slot->dbid = dbid;
		slot->pid = MyProcPid;
		memset(&slot->rep, 0, sizeof(slot->rep));
		slot->rep.start_time = GetCurrentTimestamp();
		ok = true;
		my_slot = slot;
	}
	LWLockRelease(lr_shared->lock);

	return ok;
}

/*
 * The worker's own view of itself, for gp_stat_mpp_subscription (O2).  Both
 * are no-ops when there is no slot, which is how the apply machinery stays
 * runnable outside a background worker.
 */
void
LrStatSetPhase(LrWorkerPhase phase)
{
	if (my_slot == NULL)
		return;
	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	my_slot->rep.phase = phase;
	LWLockRelease(lr_shared->lock);
}

void
LrStatPublish(const LrWorkerReport *rep)
{
	TimestampTz start;

	if (my_slot == NULL)
		return;
	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	start = my_slot->rep.start_time;	/* set once, at attach */
	memcpy(&my_slot->rep, rep, sizeof(LrWorkerReport));
	my_slot->rep.start_time = start;
	LWLockRelease(lr_shared->lock);
}

const char *
LrWorkerPhaseName(LrWorkerPhase phase)
{
	switch (phase)
	{
		case LR_PHASE_STARTING:
			return "starting";
		case LR_PHASE_INITIAL_SYNC:
			return "initial sync";
		case LR_PHASE_STREAMING:
			return "streaming";
		case LR_PHASE_TABLE_SYNC:
			return "table sync";
		case LR_PHASE_STOPPING:
			return "stopping";
	}
	return "unknown";
}

static void
lr_worker_detach(int code, Datum arg)
{
	LrWorkerSlot *slot;

	if (lr_shared == NULL)
		return;

	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	slot = slot_find(DatumGetObjectId(arg));
	if (slot != NULL && slot->pid == MyProcPid)
		slot->pid = 0;
	LWLockRelease(lr_shared->lock);
	my_slot = NULL;
}

/*
 * Wake the launcher rather than let it sleep out its nap: a subscription
 * that has just been created or enabled should not wait up to
 * mpp_logicalrep.launcher_naptime_ms for a worker.  SIGUSR1 reaches its
 * latch through the background worker's own handler, as the server's own
 * launcher is woken.
 *
 * It has to wait for commit, though.  The launcher decides from
 * pg_subscription and from the registry, and neither says anything until
 * this transaction is committed -- a launcher woken any earlier would scan,
 * find nothing, and go back to sleep for the whole nap, which is the very
 * thing this avoids.
 */
static bool launcher_wakeup_pending = false;

static void
lr_launcher_wakeup_now(void)
{
	int			pid;

	if (lr_shared == NULL)
		return;
	LWLockAcquire(lr_shared->lock, LW_SHARED);
	pid = lr_shared->launcher_pid;
	LWLockRelease(lr_shared->lock);
	if (pid != 0)
		kill(pid, SIGUSR1);
}

static void
lr_launcher_xact_cb(XactEvent event, void *arg)
{
	if (!launcher_wakeup_pending)
		return;
	if (event == XACT_EVENT_COMMIT)
	{
		launcher_wakeup_pending = false;
		lr_launcher_wakeup_now();
	}
	else if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT)
		launcher_wakeup_pending = false;
}

static void
lr_launcher_wakeup(void)
{
	launcher_wakeup_pending = true;
}

/* the worker's verdict on a subscription it could not find in the registry */
static void
lr_worker_disown(Oid subid)
{
	LrWorkerSlot *slot;

	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	slot = slot_find(subid);
	if (slot != NULL)
		slot->not_ours = true;
	LWLockRelease(lr_shared->lock);
}

/* ------------------------------------------------------------------------
 * The subscription registry
 * ------------------------------------------------------------------------
 *
 * One row per subscription this module owns, in pg_catalog of the
 * subscription's own database: gp_mpp_subscription is created by the
 * cdb_init.d script at initdb, which is what makes it a coordinator-only
 * table, so these reads and writes stay local to the coordinator and in the
 * caller's transaction.  Read by the worker, which has a connection to it.
 */
static bool
registry_says_enabled(Oid subid, bool *found)
{
	bool		enabled = false;

	*found = false;

	if (SPI_connect() != SPI_OK_CONNECT)
		return false;
	{
		Oid			argtype = OIDOID;
		Datum		arg = ObjectIdGetDatum(subid);
		int			ret;

		ret = SPI_execute_with_args("SELECT enabled FROM pg_catalog.gp_mpp_subscription"
									" WHERE subid = $1",
									1, &argtype, &arg, NULL, true, 1);
		if (ret == SPI_OK_SELECT && SPI_processed == 1)
		{
			bool		isnull;
			Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
										  SPI_tuptable->tupdesc, 1, &isnull);

			*found = true;
			enabled = !isnull && DatumGetBool(d);
		}
	}
	SPI_finish();

	return enabled;
}

/*
 * Is this subscription still one we should be applying?  Called by a running
 * worker between batches; it may or may not already be in a transaction.
 */
bool
LrSubscriptionEnabled(Oid subid)
{
	bool		started_txn = false;
	bool		found;
	bool		enabled;

	if (!IsTransactionState())
	{
		StartTransactionCommand();
		started_txn = true;
	}
	PushActiveSnapshot(GetTransactionSnapshot());

	enabled = registry_says_enabled(subid, &found);

	PopActiveSnapshot();
	if (started_txn)
		CommitTransactionCommand();

	return found && enabled;
}

/*
 * The worker's own startup check: claim a slot, and confirm the subscription
 * is ours and enabled.  Exits the process when it is not.
 */
void
LrWorkerClaim(Oid subid, Oid dbid)
{
	bool		found;
	bool		enabled;

	if (!lr_worker_attach(subid, dbid))
	{
		ereport(LOG,
				(errmsg("no mpp_logicalrep worker slot free for subscription %u",
						subid),
				 errhint("You might need to increase \"mpp_logicalrep.max_workers\".")));
		proc_exit(0);
	}
	on_shmem_exit(lr_worker_detach, ObjectIdGetDatum(subid));

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	enabled = registry_says_enabled(subid, &found);
	PopActiveSnapshot();
	CommitTransactionCommand();

	if (!found)
	{
		/* someone else's subscription; do not probe it again */
		lr_worker_disown(subid);
		proc_exit(0);
	}
	if (!enabled)
		proc_exit(0);
}

/* Ask a running worker to stop, and wait a little for it to go. */
static void
lr_worker_stop(Oid subid)
{
	int			pid = 0;
	int			i;
	LrWorkerSlot *slot;

	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	slot = slot_find(subid);
	if (slot != NULL)
	{
		pid = slot->pid;
		slot->subid = InvalidOid;	/* forget it entirely */
		slot->pid = 0;
		slot->not_ours = false;
	}
	LWLockRelease(lr_shared->lock);

	if (pid == 0)
		return;

	kill(pid, SIGTERM);
	for (i = 0; i < 100; i++)	/* up to 10s */
	{
		if (kill(pid, 0) != 0)
			break;
		pg_usleep(100000L);
		CHECK_FOR_INTERRUPTS();
	}
}

/* ------------------------------------------------------------------------
 * The subscription DDL
 * ------------------------------------------------------------------------
 */

/* Is this subscription in our registry at all?  Runs in the user's session. */
static bool
subscription_is_ours(Oid subid)
{
	bool		found = false;

	if (!OidIsValid(subid))
		return false;
	(void) registry_says_enabled(subid, &found);
	return found;
}

static void
registry_exec(const char *sql, int nargs, Oid *argtypes, Datum *values)
{
	SPI_connect();
	SPI_execute_with_args(sql, nargs, argtypes, values, NULL, false, 0);
	SPI_finish();
}

/*
 * What this subscription wants done about published tables the publisher
 * cannot decode.  The strict default also covers "no row yet".
 */
static LrUnsupportedTables
registry_unsupported(Oid subid)
{
	LrUnsupportedTables mode = LR_UNSUPPORTED_ERROR;
	Oid			argtypes[1] = {OIDOID};
	Datum		values[1] = {ObjectIdGetDatum(subid)};
	int			ret;

	SPI_connect();
	ret = SPI_execute_with_args("SELECT unsupported_tables FROM pg_catalog.gp_mpp_subscription"
								" WHERE subid = $1",
								1, argtypes, values, NULL, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool		isnull;
		Datum		d = SPI_getbinval(SPI_tuptable->vals[0],
									  SPI_tuptable->tupdesc, 1, &isnull);

		if (!isnull)
			(void) LrUnsupportedTablesParse(TextDatumGetCString(d), &mode);
	}
	SPI_finish();
	return mode;
}

/*
 * The publication's table list, as relids of this database.  The stock
 * AlterSubscription_refresh() does this and then writes pg_subscription_rel;
 * here the state table is our own and the copying is the apply worker's job,
 * so only the list is wanted.
 */
static List *
fetch_pub_relids(Subscription *sub)
{
	WalReceiverConn *wrconn;
	WalRcvExecResult *res;
	TupleTableSlot *slot;
	StringInfoData cmd;
	Oid			tableRow[3] = {TEXTOID, TEXTOID, TEXTOID};
	List	   *relids = NIL;
	List	   *unsupported = NIL;
	ListCell   *lc;
	char	   *err;
	bool		first = true;

	if (!WalReceiverFunctions)
		libpqwalreceiver_PG_init();

	wrconn = walrcv_connect(sub->conninfo, true,
							!superuser_arg(sub->owner) && sub->passwordrequired,
							sub->name, &err);
	if (!wrconn)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the publisher: %s", err)));

	/*
	 * The access method comes along so that a table whose changes the
	 * publisher cannot decode is caught by the statement that adds it,
	 * rather than by the apply worker minutes later.
	 */
	initStringInfo(&cmd);
	appendStringInfoString(&cmd,
						   "SELECT DISTINCT t.schemaname, t.tablename,"
						   "       coalesce(am.amname, 'none')"
						   "  FROM pg_catalog.pg_publication_tables t"
						   "  JOIN pg_catalog.pg_namespace n ON n.nspname = t.schemaname"
						   "  JOIN pg_catalog.pg_class c"
						   "    ON c.relnamespace = n.oid AND c.relname = t.tablename"
						   "  LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam"
						   " WHERE t.pubname IN (");
	foreach(lc, sub->publications)
	{
		appendStringInfo(&cmd, "%s%s", first ? "" : ", ",
						 quote_literal_cstr(strVal(lfirst(lc))));
		first = false;
	}
	appendStringInfoChar(&cmd, ')');

	PG_TRY();
	{
		res = walrcv_exec(wrconn, cmd.data, 3, tableRow);
		if (res->status != WALRCV_OK_TUPLES)
			ereport(ERROR,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not receive list of replicated tables from the publisher: %s",
							res->err)));

		slot = MakeSingleTupleTableSlot(res->tupledesc, &TTSOpsMinimalTuple);
		while (tuplestore_gettupleslot(res->tuplestore, true, false, slot))
		{
			bool		isnull;
			char	   *nspname = TextDatumGetCString(slot_getattr(slot, 1, &isnull));
			char	   *relname = TextDatumGetCString(slot_getattr(slot, 2, &isnull));
			char	   *amname = TextDatumGetCString(slot_getattr(slot, 3, &isnull));
			RangeVar   *rv = makeRangeVar(nspname, relname, -1);

			relids = lappend_oid(relids,
								 RangeVarGetRelid(rv, AccessShareLock, false));
			if (strcmp(amname, "heap") != 0)
				unsupported = lappend(unsupported,
									  psprintf("%s.%s (%s)", nspname, relname, amname));
			ExecClearTuple(slot);
		}
		ExecDropSingleTupleTableSlot(slot);
		walrcv_clear_result(res);
	}
	PG_FINALLY();
	{
		walrcv_disconnect(wrconn);
	}
	PG_END_TRY();

	pfree(cmd.data);

	if (unsupported != NIL)
	{
		StringInfoData names;
		bool		sep = false;

		initStringInfo(&names);
		foreach(lc, unsupported)
		{
			appendStringInfo(&names, "%s%s", sep ? ", " : "", (char *) lfirst(lc));
			sep = true;
		}
		if (registry_unsupported(sub->oid) == LR_UNSUPPORTED_ERROR)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("the publication contains tables whose changes are not decoded: %s",
							names.data),
					 errdetail("Only heap tables produce logical decoding output on the publisher. Such a table would be copied once and then never change again, with no error to show for it."),
					 errhint("Leave them out of the publication, or accept the one-time copy with ALTER SUBSCRIPTION %s SET (unsupported_tables = 'snapshot').",
							 quote_identifier(sub->name))));
		ereport(WARNING,
				(errmsg("the publication contains tables whose changes are not decoded: %s",
						names.data),
				 errdetail("They are copied once and then left alone; gp_subscription_rel records them in state 's'.")));
		pfree(names.data);
	}

	return relids;
}

/*
 * ALTER SUBSCRIPTION ... REFRESH PUBLICATION of one of ours: diff the
 * publication's table list into our state table and let the apply worker
 * copy what is new (mpp_tablesync.c).  Nothing here is non-transactional,
 * unlike the stock path.
 */
static void
refresh_mpp(Subscription *sub, bool copy_data)
{
	if (Gp_role == GP_ROLE_EXECUTE)
		return;
	LrSyncRefresh(sub->oid, fetch_pub_relids(sub), copy_data);
}

/* the value of a boolean option in a DefElem list, or a default */
static bool
option_bool(List *options, const char *name, bool defval)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, name) == 0)
			return defGetBoolean(def);
	}
	return defval;
}

/* ------------------------------------------------------------------------
 * ProcessUtility: the subscription statements
 * ------------------------------------------------------------------------
 */
static void
mpp_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
			ProcessUtilityContext context, ParamListInfo params,
			QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;
	const char *created = NULL;
	bool		created_nocopy = false;
	bool		created_enabled = true;
	LrUnsupportedTables created_unsupported = LR_UNSUPPORTED_ERROR;
	Oid			dropped = InvalidOid;
	bool		dispatch = (Gp_role == GP_ROLE_DISPATCH);

	if (dispatch && IsA(parsetree, CreateSubscriptionStmt))
	{
		CreateSubscriptionStmt *stmt = (CreateSubscriptionStmt *) parsetree;
		List	   *keep = NIL;
		ListCell   *lc;
		bool		has_slot = false;
		bool		enabled = true;
		bool		copy_data = true;
		LrUnsupportedTables unsupported = LR_UNSUPPORTED_ERROR;

		/*
		 * Every subscription created where this module is loaded is applied
		 * by it; there is no option for that and no way to ask for one of the
		 * server's own, which on Cloudberry would write its rows on the
		 * coordinator where no user table has any.
		 *
		 * The options the module owns are taken out of the statement here and
		 * acted on below rather than by the server, which would understand
		 * them as instructions to do the very things the module replaces.
		 */
		foreach(lc, stmt->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "slot_name") == 0)
			{
				has_slot = (def->arg != NULL &&
							pg_strcasecmp(defGetString(def), "none") != 0);
				keep = lappend(keep, def);
				continue;
			}
			if (strcmp(def->defname, "enabled") == 0)
			{
				/* ours: the server's row stays disabled either way */
				enabled = defGetBoolean(def);
				continue;
			}
			if (strcmp(def->defname, "copy_data") == 0)
			{
				/* ours: the initial copy is mpp_tablesync.c's, not the server's */
				copy_data = defGetBoolean(def);
				continue;
			}
			if (strcmp(def->defname, "unsupported_tables") == 0)
			{
				/* ours: the server has no notion of an undecodable table */
				if (!LrUnsupportedTablesParse(defGetString(def), &unsupported))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("\"unsupported_tables\" must be \"error\" or \"snapshot\""),
							 errdetail("\"error\" refuses a published table whose changes the publisher cannot decode; \"snapshot\" copies it once and stops tracking it.")));
				continue;
			}
			keep = lappend(keep, def);
		}

		{
			ListCell   *lc2;

			foreach(lc2, keep)
			{
				DefElem    *def = (DefElem *) lfirst(lc2);

				/*
				 * connect is how the module keeps the server out of the way
				 * (below), so it is not the user's to set -- in either
				 * direction.  Saying so plainly beats the server's "
				 * conflicting or redundant options" from a duplicate.
				 */
				if (strcmp(def->defname, "connect") == 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("\"connect\" is not accepted for a subscription applied by mpp_logicalrep"),
							 errdetail("The module sets it itself: the statement must not connect to the publisher, must not create a slot, and must leave the pg_subscription row disabled so that the server's own launcher never starts a worker for it."),
							 errhint("Leave it out.  A subscription dumped by pg_dump carries \"connect = false\" and has to have it removed, or be dumped with --no-subscriptions.")));

				/*
				 * The slots are cluster-wide -- one per primary segment plus
				 * the coordinator, under one name, with two-phase decoding on
				 * -- so only gp_create_logical_replication_slot() can make
				 * them, and only before the subscription exists.
				 */
				if (strcmp(def->defname, "create_slot") == 0 && defGetBoolean(def))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("\"create_slot = true\" is not supported for a subscription applied by mpp_logicalrep"),
							 errdetail("The publisher needs a slot on every primary segment and on the coordinator, under one name and with two-phase decoding enabled, which this statement cannot create."),
							 errhint("Create them on the publisher first with gp_create_logical_replication_slot('<name>', 'pgoutput', false, true), then name them here with slot_name.")));
			}

			if (!has_slot)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("a subscription applied by mpp_logicalrep requires slot_name"),
						 errdetail("It names the cluster-wide slots on the publisher, which the subscription's readers stream from."),
						 errhint("Create them with gp_create_logical_replication_slot('<name>', 'pgoutput', false, true) on the publisher.")));

			/*
			 * Leave the catalog row disabled and unconnected: the server's
			 * launcher then ignores the subscription, no slot is created
			 * behind the user's back, no table list is fetched, and this
			 * extension's launcher is the only thing that drives it.
			 */
			keep = lappend(keep,
						   makeDefElem("connect",
									   (Node *) makeString("false"), -1));
			stmt->options = keep;
			created = stmt->subname;
			created_enabled = enabled;
			created_unsupported = unsupported;

			/*
			 * copy_data = false means the tables are declared in sync, so
			 * the worker streams at once instead of copying.
			 */
			created_nocopy = !copy_data;
		}
	}
	else if (dispatch && IsA(parsetree, AlterSubscriptionStmt))
	{
		AlterSubscriptionStmt *stmt = (AlterSubscriptionStmt *) parsetree;
		Oid			subid = get_subscription_oid(stmt->subname, true);

		if (subscription_is_ours(subid))
		{
			Subscription *sub = GetSubscription(subid, false);

			switch (stmt->kind)
			{
				case ALTER_SUBSCRIPTION_ENABLED:
					{
						/*
						 * Our subscriptions stay disabled in pg_subscription
						 * -- that is what keeps the server's own launcher off
						 * them -- so ENABLE and DISABLE act on the registry
						 * and the statement stops here.
						 */
						bool		enable = option_bool(stmt->options, "enabled", true);
						Oid			argtypes[1] = {OIDOID};
						Datum		values[1];

						values[0] = ObjectIdGetDatum(subid);
						if (enable)
						{
							registry_exec("UPDATE pg_catalog.gp_mpp_subscription"
										  " SET enabled = true, disable_reason = NULL"
										  " WHERE subid = $1",
										  1, argtypes, values);
							lr_launcher_wakeup();
						}
						else
						{
							registry_exec("UPDATE pg_catalog.gp_mpp_subscription"
										  " SET enabled = false WHERE subid = $1",
										  1, argtypes, values);
							lr_worker_stop(subid);
						}
						return;
					}

				case ALTER_SUBSCRIPTION_OPTIONS:
					{
						/*
						 * unsupported_tables is ours; anything else in the
						 * same SET is still the server's to apply.
						 */
						List	   *keep = NIL;
						ListCell   *lc2;
						bool		seen = false;
						LrUnsupportedTables mode = LR_UNSUPPORTED_ERROR;

						foreach(lc2, stmt->options)
						{
							DefElem    *def = (DefElem *) lfirst(lc2);

							if (strcmp(def->defname, "unsupported_tables") == 0)
							{
								if (!LrUnsupportedTablesParse(defGetString(def), &mode))
									ereport(ERROR,
											(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
											 errmsg("\"unsupported_tables\" must be \"error\" or \"snapshot\""),
											 errdetail("\"error\" refuses a published table whose changes the publisher cannot decode; \"snapshot\" copies it once and stops tracking it.")));
								seen = true;
								continue;
							}
							keep = lappend(keep, def);
						}
						if (seen)
						{
							Oid			argtypes[2] = {TEXTOID, OIDOID};
							Datum		values[2];

							values[0] = CStringGetTextDatum(LrUnsupportedTablesName(mode));
							values[1] = ObjectIdGetDatum(subid);
							registry_exec("UPDATE pg_catalog.gp_mpp_subscription"
										  " SET unsupported_tables = $1 WHERE subid = $2",
										  2, argtypes, values);
							if (keep == NIL)
								return; /* nothing left for the server */
							stmt->options = keep;
						}
						break;
					}

				case ALTER_SUBSCRIPTION_REFRESH:
					refresh_mpp(sub, option_bool(stmt->options, "copy_data", true));
					return;

				case ALTER_SUBSCRIPTION_SET_PUBLICATION:
				case ALTER_SUBSCRIPTION_ADD_PUBLICATION:
				case ALTER_SUBSCRIPTION_DROP_PUBLICATION:
					{
						/*
						 * Let the server change the publication list, but not
						 * refresh: its refresh writes pg_subscription_rel and
						 * is non-transactional, and ours is neither.
						 */
						bool		refresh = option_bool(stmt->options, "refresh", true);

						stmt->options = lappend(stmt->options,
												makeDefElem("refresh",
															(Node *) makeString("false"),
															-1));
						if (prev_ProcessUtility)
							prev_ProcessUtility(pstmt, queryString, readOnlyTree,
												context, params, queryEnv, dest, qc);
						else
							standard_ProcessUtility(pstmt, queryString, readOnlyTree,
													context, params, queryEnv, dest, qc);
						if (refresh)
							refresh_mpp(GetSubscription(subid, false),
										option_bool(stmt->options, "copy_data", true));
						return;
					}

				default:
					break;		/* options, owner, skip: the server's business */
			}
		}
	}
	else if (dispatch && IsA(parsetree, DropSubscriptionStmt))
	{
		DropSubscriptionStmt *stmt = (DropSubscriptionStmt *) parsetree;
		Oid			subid = get_subscription_oid(stmt->subname, stmt->missing_ok);

		if (subscription_is_ours(subid))
		{
			Oid			argtypes[1] = {OIDOID};
			Datum		values[1];
			Subscription *sub = GetSubscription(subid, false);
			char	   *slotname = sub->slotname ? pstrdup(sub->slotname) : NULL;

			/*
			 * Stop the worker first, then take the slot name off the
			 * subscription: our slots are cluster-wide and the user's to
			 * manage, so DROP SUBSCRIPTION must not connect to the publisher
			 * to drop one, which is also what lets it run in a transaction
			 * block.
			 */
			values[0] = ObjectIdGetDatum(subid);
			registry_exec("UPDATE pg_catalog.gp_mpp_subscription"
						  " SET enabled = false WHERE subid = $1",
						  1, argtypes, values);
			lr_worker_stop(subid);

			SPI_connect();
			SPI_execute(psprintf("ALTER SUBSCRIPTION %s SET (slot_name = NONE)",
								 quote_identifier(stmt->subname)), false, 0);
			SPI_finish();

			/*
			 * The slots outlive the subscription because they were never
			 * ours to create.  Say so: a forgotten set of slots holds down
			 * the WAL of every primary segment of the publisher.
			 */
			if (slotname != NULL)
				ereport(NOTICE,
						(errmsg("the replication slots named \"%s\" are left on the publisher",
								slotname),
						 errhint("Drop them there with gp_drop_replication_slot('%s') once no subscription needs them; until then the publisher keeps the WAL they hold.",
								 slotname)));

			dropped = subid;
		}
	}

	if (prev_ProcessUtility)
		prev_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
							queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree, context, params,
								queryEnv, dest, qc);

	if (created != NULL)
	{
		Oid			subid = get_subscription_oid(created, false);
		Oid			argtypes[4] = {OIDOID, TEXTOID, BOOLOID, TEXTOID};
		Datum		values[4];

		values[0] = ObjectIdGetDatum(subid);
		values[1] = CStringGetTextDatum(created);
		values[2] = BoolGetDatum(created_enabled);
		values[3] = CStringGetTextDatum(LrUnsupportedTablesName(created_unsupported));
		registry_exec("INSERT INTO pg_catalog.gp_mpp_subscription"
					  " (subid, subname, enabled, unsupported_tables)"
					  " VALUES ($1, $2, $3, $4)"
					  " ON CONFLICT (subid) DO UPDATE"
					  " SET subname = EXCLUDED.subname,"
					  "     enabled = EXCLUDED.enabled,"
					  "     unsupported_tables = EXCLUDED.unsupported_tables,"
					  "     disable_reason = NULL",
					  4, argtypes, values);

		if (created_nocopy)
			LrSyncMarkNoCopy(subid);

		if (created_enabled)
		{
			lr_launcher_wakeup();
			ereport(NOTICE,
					(errmsg("subscription \"%s\" will be applied by mpp_logicalrep",
							created),
					 errdetail("Its pg_subscription row stays disabled so that the server's own apply worker never starts; pg_catalog.gp_mpp_subscription is what enables it.")));
		}
		else
			ereport(NOTICE,
					(errmsg("subscription \"%s\" will be applied by mpp_logicalrep, and is not enabled",
							created),
					 errhint("Start it with ALTER SUBSCRIPTION %s ENABLE.",
							 quote_identifier(created))));
	}
	else if (OidIsValid(dropped))
	{
		Oid			argtypes[1] = {OIDOID};
		Datum		values[1];

		LrSyncCleanup(dropped);
		values[0] = ObjectIdGetDatum(dropped);
		registry_exec("DELETE FROM pg_catalog.gp_mpp_subscription WHERE subid = $1",
					  1, argtypes, values);
	}
}

/* ------------------------------------------------------------------------
 * What the workers are doing
 *
 * Two set-returning functions over the shared-memory slots, which
 * gp_stat_mpp_subscription and gp_stat_mpp_subscription_segments join with
 * the state tables.  They report only the workers of the current database,
 * because the registry and the state tables they are read against are
 * per-database.
 * ------------------------------------------------------------------------
 */

#define LR_STAT_NCOLS		22
#define LR_STAT_SEG_NCOLS	6

/* the set-returning boilerplate, once */
static Tuplestorestate *
stat_begin(FunctionCallInfo fcinfo, TupleDesc *tupdesc)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext oldcxt;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo) ||
		(rsinfo->allowedModes & SFRM_Materialize) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("this function must be called in a context that accepts a set")));
	if (get_call_result_type(fcinfo, NULL, tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	oldcxt = MemoryContextSwitchTo(rsinfo->econtext->ecxt_per_query_memory);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = *tupdesc;
	MemoryContextSwitchTo(oldcxt);

	return tupstore;
}

PG_FUNCTION_INFO_V1(gp_stat_get_mpp_subscription);
Datum
gp_stat_get_mpp_subscription(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore = stat_begin(fcinfo, &tupdesc);
	int			i;

	if (lr_shared == NULL)
		return (Datum) 0;

	LWLockAcquire(lr_shared->lock, LW_SHARED);
	for (i = 0; i < lr_shared->nslots; i++)
	{
		LrWorkerSlot *slot = &lr_shared->slots[i];
		LrWorkerReport *r = &slot->rep;
		Datum		values[LR_STAT_NCOLS];
		bool		nulls[LR_STAT_NCOLS];
		int64		buffered_txns = 0;
		int64		buffered_bytes = 0;
		int			c = 0;
		int			seg;

		if (!OidIsValid(slot->subid) || slot->pid == 0 ||
			slot->dbid != MyDatabaseId)
			continue;

		for (seg = 0; seg < r->nsrcsegs && seg < LR_STAT_MAX_SEGS; seg++)
		{
			buffered_txns += r->seg_buffered_txns[seg];
			buffered_bytes += r->seg_buffered_bytes[seg];
		}

		memset(nulls, 0, sizeof(nulls));
		values[c++] = ObjectIdGetDatum(slot->subid);
		values[c++] = Int32GetDatum(slot->pid);
		values[c++] = CStringGetTextDatum(LrWorkerPhaseName(r->phase));
		values[c++] = TimestampTzGetDatum(r->start_time);
		values[c++] = Int32GetDatum(r->nsrcsegs);
		/*
		 * A zero position is one nothing has reported yet -- no batch has
		 * been committed, no keepalive has arrived.  Reporting it as 0/0
		 * would make the lag against it look enormous, so say nothing.
		 */
		if (r->received_lsn == 0)
			nulls[c] = true;
		values[c++] = LSNGetDatum((XLogRecPtr) r->received_lsn);
		if (r->applied_lsn == 0)
			nulls[c] = true;
		values[c++] = LSNGetDatum((XLogRecPtr) r->applied_lsn);
		if (r->last_msg_time == 0)
			nulls[c] = true;
		values[c++] = TimestampTzGetDatum(r->last_msg_time);
		if (r->last_commit_time == 0)
			nulls[c] = true;
		values[c++] = TimestampTzGetDatum(r->last_commit_time);
		values[c++] = Int64GetDatum(r->awaited_txns);
		values[c++] = Int64GetDatum(r->deferred_txns);
		values[c++] = Int64GetDatum(r->batch_txns);
		values[c++] = Int64GetDatum(r->batch_bytes);
		values[c++] = Int64GetDatum(buffered_txns);
		values[c++] = Int64GetDatum(buffered_bytes);
		values[c++] = Int64GetDatum(r->stuck_errors);
		values[c++] = Int64GetDatum(r->stats.events);
		values[c++] = Int64GetDatum(r->stats.batches);
		values[c++] = Int64GetDatum(r->stats.txns_applied);
		values[c++] = Int64GetDatum(r->stats.local_applied);
		values[c++] = Int64GetDatum(r->stats.rows_applied);
		values[c++] = Int64GetDatum(r->stats.recoveries);
		Assert(c == LR_STAT_NCOLS);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	LWLockRelease(lr_shared->lock);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(gp_stat_get_mpp_subscription_segs);
Datum
gp_stat_get_mpp_subscription_segs(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore = stat_begin(fcinfo, &tupdesc);
	int			i;

	if (lr_shared == NULL)
		return (Datum) 0;

	LWLockAcquire(lr_shared->lock, LW_SHARED);
	for (i = 0; i < lr_shared->nslots; i++)
	{
		LrWorkerSlot *slot = &lr_shared->slots[i];
		LrWorkerReport *r = &slot->rep;
		int			seg;

		if (!OidIsValid(slot->subid) || slot->pid == 0 ||
			slot->dbid != MyDatabaseId)
			continue;

		for (seg = 0; seg < r->nsrcsegs && seg < LR_STAT_MAX_SEGS; seg++)
		{
			Datum		values[LR_STAT_SEG_NCOLS];
			bool		nulls[LR_STAT_SEG_NCOLS];
			int			c = 0;

			memset(nulls, 0, sizeof(nulls));
			values[c++] = ObjectIdGetDatum(slot->subid);
			values[c++] = Int32GetDatum(seg);
			if (r->seg_pos[seg] == 0)
				nulls[c] = true;	/* nothing read from it yet */
			values[c++] = LSNGetDatum((XLogRecPtr) r->seg_pos[seg]);
			if (r->seg_flush[seg] == 0)
				nulls[c] = true;	/* replay starts wherever the slot is */
			values[c++] = LSNGetDatum((XLogRecPtr) r->seg_flush[seg]);
			values[c++] = Int64GetDatum(r->seg_buffered_txns[seg]);
			values[c++] = Int64GetDatum(r->seg_buffered_bytes[seg]);
			Assert(c == LR_STAT_SEG_NCOLS);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}
	LWLockRelease(lr_shared->lock);

	return (Datum) 0;
}

/* ------------------------------------------------------------------------
 * The launcher
 * ------------------------------------------------------------------------
 */
PGDLLEXPORT void MppLauncherMain(Datum main_arg);

static void
lr_launcher_detach(int code, Datum arg)
{
	if (lr_shared == NULL)
		return;
	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	if (lr_shared->launcher_pid == MyProcPid)
		lr_shared->launcher_pid = 0;
	LWLockRelease(lr_shared->lock);
}

static void
launch_worker(Oid dbid, Oid subid, Oid userid, const char *subname)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *handle;
	LrWorkerArg arg;

	memset(&arg, 0, sizeof(arg));
	arg.dbid = dbid;
	arg.subid = subid;
	arg.userid = userid;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "mpp_logicalrep");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "MppApplyWorkerMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN,
			 "mpp apply worker for subscription \"%s\"", subname);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "mpp apply worker");
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = 0;
	memcpy(bgw.bgw_extra, &arg, sizeof(arg));

	if (!RegisterDynamicBackgroundWorker(&bgw, &handle))
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("out of background worker slots"),
				 errhint("You might need to increase \"max_worker_processes\".")));
	else
		pfree(handle);
}

/*
 * Subscriptions that could be ours: disabled (which is how the utility hook
 * leaves them) and with a slot name.  pg_subscription is a shared catalog,
 * so this needs no database connection -- which is what lets one launcher
 * serve every database, as the server's own does.
 */
static void
launch_due_workers(void)
{
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tup;
	List	   *due = NIL;
	ListCell   *lc;
	MemoryContext oldctx;

	StartTransactionCommand();

	rel = table_open(SubscriptionRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);
	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_subscription subform = (Form_pg_subscription) GETSTRUCT(tup);
		LrWorkerSlot *slot;
		bool		start;
		LrWorkerArg *arg;
		Datum		d;
		bool		isnull;

		if (subform->subenabled)
			continue;			/* the server's own launcher owns that one */

		d = heap_getattr(tup, Anum_pg_subscription_subslotname,
						 RelationGetDescr(rel), &isnull);
		if (isnull)
			continue;
		(void) d;

		LWLockAcquire(lr_shared->lock, LW_SHARED);
		slot = slot_find(subform->oid);
		start = (slot == NULL || (slot->pid == 0 && !slot->not_ours));
		LWLockRelease(lr_shared->lock);

		if (!start)
			continue;

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		arg = palloc0(sizeof(LrWorkerArg));
		arg->dbid = subform->subdbid;
		arg->subid = subform->oid;
		arg->userid = subform->subowner;
		namestrcpy(&arg->subname, NameStr(subform->subname));
		due = lappend(due, arg);
		MemoryContextSwitchTo(oldctx);
	}
	table_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();

	foreach(lc, due)
	{
		LrWorkerArg *arg = (LrWorkerArg *) lfirst(lc);

		launch_worker(arg->dbid, arg->subid, arg->userid, NameStr(arg->subname));
		pfree(arg);
	}
	list_free(due);
}

void
MppLauncherMain(Datum main_arg)
{
	bool		stopping_said = false;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/*
	 * The driver replicates into every segment from the coordinator, so it
	 * runs there and nowhere else.  A segment's own catalog carries the same
	 * subscription row after dispatch, and must not act on it.
	 *
	 * Nor does a coordinator started in utility mode count.  gpstart brings
	 * one up that way to do its own bootstrapping and stops it again a second
	 * later, and a worker started in it has no gangs to drive and no way to
	 * reach the segments: it would read the source as having lost most of its
	 * slots and disable the subscription over it, in a postmaster that is
	 * about to go away.  gp_role is fixed for a postmaster's life, so there
	 * is nothing here to wait for.
	 */
	if (!IS_QUERY_DISPATCHER() || Gp_role != GP_ROLE_DISPATCH)
		proc_exit(0);

	/* no database: pg_subscription is a shared catalog */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	LWLockAcquire(lr_shared->lock, LW_EXCLUSIVE);
	lr_shared->launcher_pid = MyProcPid;
	LWLockRelease(lr_shared->lock);
	on_shmem_exit(lr_launcher_detach, (Datum) 0);

	for (;;)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		/*
		 * A worker that noticed a smart shutdown stopped itself; starting it
		 * again would put its session back and leave the shutdown waiting on
		 * the very thing it just gave up.  Keep napping until the postmaster
		 * gets round to signalling background workers.
		 */
		if (LrPostmasterStopping())
		{
			if (!stopping_said)
			{
				ereport(LOG,
						(errmsg("mpp logical replication launcher: the server is shutting down, starting no more workers")));
				stopping_said = true;
			}
		}
		else
			launch_due_workers();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   launcher_naptime_ms, PG_WAIT_EXTENSION);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/* ------------------------------------------------------------------------
 * Module load
 * ------------------------------------------------------------------------
 */
void
_PG_init(void)
{
	BackgroundWorker bgw;

	define_gucs();

	/*
	 * Custom scan providers of the apply plans.  Every backend may have to
	 * deserialize such a plan on dispatch, which is why this library belongs
	 * in shared_preload_libraries: registering in the postmaster registers
	 * for every backend it forks.
	 */
	LrRegisterCustomScanMethods();

	prev_ProcessUtility = ProcessUtility_hook;
	ProcessUtility_hook = mpp_utility;

	RegisterXactCallback(lr_launcher_xact_cb, NULL);

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("mpp_logicalrep.max_workers",
							"Maximum number of MPP apply workers.",
							NULL,
							&max_workers,
							8, 1, 1024,
							PGC_POSTMASTER, 0, NULL, NULL, NULL);

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = lr_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = lr_shmem_startup;

	memset(&bgw, 0, sizeof(bgw));
	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(bgw.bgw_library_name, BGW_MAXLEN, "mpp_logicalrep");
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "MppLauncherMain");
	snprintf(bgw.bgw_name, BGW_MAXLEN, "mpp logical replication launcher");
	snprintf(bgw.bgw_type, BGW_MAXLEN, "mpp logical replication launcher");
	bgw.bgw_restart_time = 5;
	bgw.bgw_notify_pid = 0;
	RegisterBackgroundWorker(&bgw);
}
