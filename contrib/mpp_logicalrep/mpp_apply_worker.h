/*-------------------------------------------------------------------------
 *
 * mpp_apply_worker.h
 *	  The apply driver of MPP logical replication (design §7.2, §8.3).
 *
 * The driver is the only process that reads the source coordinator's
 * logical stream.  Every "cbdb.dcommit" event names a committed
 * distributed transaction; the driver asks the reader gang for it
 * (gp_lr_drain), batches ready transactions, and applies each batch in one
 * distributed transaction of the target (gp_lr_apply + gp_lr_progress).
 * It drives a normal coordinator session through libpq so that the plans
 * are dispatched with a two-phase context; the driver process itself
 * touches no table.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * contrib/mpp_logicalrep/mpp_apply_worker.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MPP_APPLY_WORKER_H
#define MPP_APPLY_WORKER_H

#include "nodes/pg_list.h"

/*
 * What to do when the subscription's replication slots are gone from the
 * source (a segment failover loses them: logical slots are local to the
 * primary and PG16 has no slot sync).  Design section 13.2.
 */
typedef enum LrSlotLossAction
{
	LR_SLOT_LOSS_REINIT = 0,	/* redo the whole subscription from scratch */
	LR_SLOT_LOSS_DISABLE		/* stop and let an operator decide */
} LrSlotLossAction;

/*
 * What to do when an apply error keeps coming back.  Any error is retried
 * from the last committed position, which is right for a lost connection or
 * a reset gang but useless for a configuration the target cannot apply (a
 * missing relation, a replica identity that does not cover what apply needs):
 * that one fails again on every replay, for ever.
 */
typedef enum LrApplyErrorAction
{
	LR_APPLY_ERROR_DISABLE = 0, /* stop the subscription and say why */
	LR_APPLY_ERROR_RETRY		/* keep trying (the old behaviour) */
} LrApplyErrorAction;

/* GUCs (guc_gp.c) */
extern int	gp_lr_slot_loss_action;
extern int	gp_lr_apply_error_action;
extern int	gp_lr_apply_max_retries;
extern int	gp_lr_batch_max_txns;
extern int	gp_lr_batch_max_bytes;	/* kB */
extern int	gp_lr_batch_max_latency_ms;
extern int	gp_lr_event_wait_ms;	/* one DRAIN's wait for its target gxid */
extern int	gp_lr_idle_drain_ms;	/* idle stream: poll readers for local txns */

typedef struct LrWorkerConfig
{
	Oid			subid;
	int64		token;

	/* libpq conninfo of the target coordinator (the driven session) */
	const char *target_conninfo;

	/* libpq conninfo of the source coordinator (walreceiver + catalog) */
	const char *source_conninfo;

	const char *subname;		/* for the DDL the worker runs on itself */
	const char *coord_slot;		/* logical slot on the source coordinator */
	const char *seg_slot;		/* logical slot on every source segment */
	List	   *publications;	/* String list */

	/*
	 * Initial-sync deduplication (design section 10): a distributed snapshot
	 * cut point.  When sync_ds_xmax != 0, a ready gxid G that is visible under
	 * this snapshot (G < sync_ds_xmax and G not in sync_ds_inprogress) was
	 * already captured by the table COPY taken at the cut point, so it is
	 * dropped instead of applied.  Self-limiting: every gxid committed after
	 * the cut point is >= sync_ds_xmax, so the filter only ever drops the
	 * pre-COPY transactions and becomes inert afterwards.
	 */
	int64		sync_ds_xmax;
	int			nsync_ds_inprogress;
	const int64 *sync_ds_inprogress;

	/*
	 * Run the initial synchronisation (mpp_tablesync.c) before streaming
	 * unless gp_subscription_rel says it is done; fills sync_ds_* above.
	 */
	bool		initial_sync;

	/* stop conditions; 0 = none */
	int			max_batches;
	int			max_seconds;
	bool		(*should_stop) (void *arg);
	void	   *stop_arg;
} LrWorkerConfig;

typedef struct LrWorkerStats
{
	int64		events;			/* dcommit events consumed */
	int64		batches;		/* batches committed */
	int64		txns_applied;	/* gxid transactions applied */
	int64		local_applied;	/* local transactions applied */
	int64		events_deduped;	/* events for already applied gxids */
	int64		events_noop;	/* events whose transaction wrote on no segment */
	int64		txns_catchup;	/* transactions replayed for an added table */
	int64		txns_catchup_dropped;	/* held ones the copy already had */
	int64		recoveries;
	int64		rows_applied;	/* as reported by the writers */
} LrWorkerStats;

extern void LrApplyWorkerRun(const LrWorkerConfig *cfg, LrWorkerStats *stats);

/* mpp_tablesync.c */
extern bool LrInitialSync(LrWorkerConfig *cfg);
extern void LrSyncMarkNoCopy(Oid subid);
extern void LrSyncCleanup(Oid subid);
extern void LrSyncRefresh(Oid subid, List *relids, bool copy_data);

/*
 * Incremental synchronisation of tables added to a running subscription
 * (design section 10.5).  The driver defers the changes of every pending
 * table, copies them under a cut point of their own while the other tables
 * keep streaming, catches them up from its buffers and only then publishes
 * them.  struct LrSync is private to mpp_tablesync.c.
 */
struct LrSync;
struct pg_conn;

extern List *LrSyncPendingNames(struct pg_conn *qd, Oid subid);
extern struct LrSync *LrSyncAddStart(LrWorkerConfig *cfg);
extern bool LrSyncAddStep(struct LrSync *job);
extern int64 LrSyncAddCutLsn(struct LrSync *job);
extern void LrSyncAddSnapshot(struct LrSync *job, int64 *xmax,
							  int *ninprogress, int64 **inprogress);
extern List *LrSyncAddNames(struct LrSync *job);
extern void LrSyncAddFinish(struct LrSync *job);
extern void LrSyncAddAbort(struct LrSync *job);

/*
 * What the launcher hands a worker through bgw_extra: everything it needs
 * before it has a database connection to look anything up with.
 */
typedef struct LrWorkerArg
{
	Oid			dbid;
	Oid			subid;
	Oid			userid;
} LrWorkerArg;

/* Background-worker entry point (the launcher in mpp_logicalrep.c) */
extern PGDLLEXPORT void MppApplyWorkerMain(Datum main_arg);

/* mpp_logicalrep.c: the extension's own subscription registry */
extern bool LrSubscriptionEnabled(Oid subid);
extern void LrWorkerClaim(Oid subid, Oid dbid);

#endif							/* MPP_APPLY_WORKER_H */
