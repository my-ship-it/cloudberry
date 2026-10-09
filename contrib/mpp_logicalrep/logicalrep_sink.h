/*-------------------------------------------------------------------------
 *
 * logicalrep_sink.h
 *	  Target-neutral apply interfaces for Cloudberry logical replication.
 *
 * The receiving side is cut in two along a line that does not depend on the
 * target:
 *
 *	  assembly	 one stream per source segment, re-ordered by gxid, released
 *				 in the commit order of the source coordinator
 *		 |
 *	  [ this contract ]
 *		 |
 *	  sink		 puts an assembled transaction into a concrete target
 *				 (Cloudberry, single-node PG, Oracle, ...)
 *
 * The driver (lr_apply_driver_run) depends on these two interfaces and on
 * nothing else, so a new target is a new sink and a new stream source is a
 * new assembly; neither one rewrites the loop.
 *
 * Contract v2.  v1 described one assembled transaction as
 * one target transaction with its rows handed to the sink row by row.  The
 * production Cloudberry sink does neither: it commits a whole unit of
 * transactions in one distributed commit, and its rows never pass through
 * the driver process at all (they travel segment to segment inside the
 * target cluster).  Both are now capability bits, and v1 is what this
 * contract collapses to when they are off: units of one transaction, rows
 * delivered through change().
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * contrib/mpp_logicalrep/logicalrep_sink.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALREP_SINK_H
#define LOGICALREP_SINK_H

#include "access/xlogdefs.h"

/* no heavy dependencies in this header */
struct TupleTableSlot;

/* row-level operation */
typedef enum LogicalRepChangeOp
{
	LR_CHANGE_INSERT,
	LR_CHANGE_UPDATE,
	LR_CHANGE_DELETE
} LogicalRepChangeOp;

/*
 * A relation's stable identity -- deliberately not a local OID.  Every sink
 * maps it its own way: Cloudberry and PG to a local OID by name, Oracle to
 * a target table name.  remote_relid relates the entry to the RELATION
 * messages of the source stream and means nothing outside it.
 */
typedef struct LogicalRepRelIdent
{
	const char *nspname;
	const char *relname;
	uint32		remote_relid;
} LogicalRepRelIdent;

/* a relation a unit writes, and the lock it needs on it */
typedef struct LogicalRepRelLock
{
	LogicalRepRelIdent rel;

	/*
	 * What the changes need if the target gives no help below the table:
	 * AccessExclusiveLock for a TRUNCATE, ExclusiveLock once anything locks
	 * rows (an UPDATE or a DELETE), RowExclusiveLock for inserts alone.
	 *
	 * It is a requirement, not an instruction.  Whether rows can be locked
	 * at all, and whether a waiting cycle among them would ever be broken,
	 * are properties of the target -- so a sink that knows its target may
	 * take less than this, and only it can know that it may.
	 */
	int			lockmode;
} LogicalRepRelLock;

/*
 * One row-level change, for sinks that take their rows from the driver.
 * INSERT has newtuple, DELETE has oldkey, UPDATE has both (the old replica
 * identity locates the row and routes it).
 */
typedef struct LogicalChange
{
	LogicalRepChangeOp op;
	LogicalRepRelIdent rel;
	struct TupleTableSlot *newtuple;
	struct TupleTableSlot *oldkey;
} LogicalChange;

/*
 * One assembled distributed transaction: complete across the source
 * segments and placed in the commit order of the source coordinator.
 *
 * A transaction that only ever existed on one source segment (a utility
 * write, which has no distributed identity) is one of these with gxid == 0;
 * it is then addressed by its position in that segment's stream alone.
 */
typedef struct AssembledTxn
{
	uint64		gxid;			/* source distributed transaction, 0 = local */
	XLogRecPtr	commit_order_pos;	/* its place in the coordinator stream */

	/*
	 * Participating source segments and this transaction's position in each
	 * one's stream.  Filled only when the sink asks for participant sets.
	 */
	int			nparticipants;
	int		   *participant_segidx;
	XLogRecPtr *participant_pos;

	/*
	 * Relations of this transaction alone that must not be applied, because
	 * whatever holds them already has its rows -- a table copy taken after
	 * this transaction committed, say.  Unlike the unit-wide sets in the
	 * transport's own spec, this one belongs to the transaction: two in the
	 * same unit can disagree about the same relation, which is what happens
	 * when each table's copy has its own cut point.  Remote relids, empty
	 * when there is nothing to skip.
	 */
	int			nskip;
	Oid		   *skip;
} AssembledTxn;

/*
 * A unit of work: the transactions the sink commits together, in commit
 * order.  A sink that does not set commits_units gets units of one.
 */
typedef struct LogicalRepUnit
{
	int			ntxns;
	AssembledTxn *txns;			/* commit order */
	XLogRecPtr	commit_order_pos;	/* the last transaction's position */

	/*
	 * The relations the unit writes and the lock each needs, filled only
	 * when the sink asks for the enumeration (a target with one lock manager
	 * does not need it, and the assembly then skips the work).
	 */
	int			nrels;
	LogicalRepRelLock *rels;

	/*
	 * Private to a pairing that moves rows out of band: the assembly hands
	 * its sink whatever that transport needs (for Cloudberry, which tables
	 * the reader gang may emit).  Opaque to the driver.
	 */
	void	   *transport_private;
} LogicalRepUnit;

/*
 * Apply progress, persisted by the sink and read back at startup.  Each
 * source segment advances on its own, so a position is a vector, not a
 * value; the applied triples deduplicate a replay that starts before them.
 *
 * nsegs == 0 means this unit moves no position and the sink persists
 * nothing for it -- a replay of transactions that were already accounted
 * for, which only has to commit.
 */
typedef struct ProgressPos
{
	XLogRecPtr	commit_order_pos;	/* coordinator stream, last committed */

	int			nsegs;
	XLogRecPtr *seg_pos;		/* consumed per source segment */
	XLogRecPtr *seg_flush;		/* replay may start here */

	int			napplied;
	int		   *applied_segidx;
	uint64	   *applied_gxid;
	XLogRecPtr *applied_pos;
} ProgressPos;

/* what commit_unit reports */
typedef enum CommitResult
{
	LR_COMMIT_OK,
	LR_COMMIT_RETRY,			/* transient (a deadlock victim): try again */
	LR_COMMIT_FATAL
} CommitResult;

typedef struct LogicalRepSink LogicalRepSink;
typedef struct LogicalRepAssembly LogicalRepAssembly;

/*
 * What the sink needs, so that the assembly and the driver do that much and
 * no more.  A single-node sink turns almost all of it off and gets a
 * markedly shorter path for free.
 */
typedef struct SinkCaps
{
	/*
	 * The driver feeds rows to change().  Off means the rows reach the
	 * target without passing through this process, and the sink is handed
	 * whole units to apply instead (Cloudberry: the reader gang sends them
	 * to the writer gang over the interconnect).  An out-of-band sink is
	 * paired with an assembly that shares its transport.
	 */
	bool		delivers_rows;

	/* many assembled transactions may share one target commit */
	bool		commits_units;

	bool		needs_participant_set;	/* which segments took part */
	bool		needs_relid_enumeration_for_lock;	/* union of relations per unit */
	bool		needs_redistribution;	/* rows routed by the target's policy */
	bool		does_2pc;		/* the target commit is two-phase */
	bool		needs_type_mapping;		/* heterogeneous target (Oracle) */
} SinkCaps;

/*
 * The sink: everything that knows what the target is.
 *
 * Life cycle, driven by lr_apply_driver_run:
 *
 *	 startup(cfg); caps = capabilities(); pos = get_checkpoint();
 *	 per unit:
 *		 begin_unit(u)					 open a target transaction, take the
 *										 locks of u->rels in a canonical order
 *		 delivers_rows ? begin_txn(t) + change(c)... per transaction
 *					   : apply_unit(u)	 the out-of-band path
 *		 commit_unit(u, pos)			 persist pos and commit as one
 *	 abort() after any error, shutdown() at the end.
 *
 * Invariants: commit order is preserved across units and inside one; a
 * transaction's changes are applied in the order of its source stream; the
 * progress a unit reports is committed with it, never after it.
 */
struct LogicalRepSink
{
	void		(*startup) (LogicalRepSink *self, void *cfg);
	void		(*shutdown) (LogicalRepSink *self);

	SinkCaps	(*capabilities) (LogicalRepSink *self);

	/* progress persisted by an earlier run (or by this one) */
	ProgressPos (*get_checkpoint) (LogicalRepSink *self);

	void		(*begin_unit) (LogicalRepSink *self, const LogicalRepUnit *unit);

	/* only when delivers_rows */
	void		(*begin_txn) (LogicalRepSink *self, const AssembledTxn *txn);
	void		(*change) (LogicalRepSink *self, const LogicalChange *chg);

	/* only when not delivers_rows */
	void		(*apply_unit) (LogicalRepSink *self, const LogicalRepUnit *unit);

	CommitResult (*commit_unit) (LogicalRepSink *self, const LogicalRepUnit *unit,
								 const ProgressPos *pos);

	/* end whatever is open; the driver then rebuilds from the checkpoint */
	void		(*abort) (LogicalRepSink *self);

	void	   *private_data;
};

/*
 * The assembly: everything that knows where the stream comes from.  It
 * tailors itself to the sink's capabilities at startup and hands out units
 * only when they are complete and it is their turn.
 */
struct LogicalRepAssembly
{
	/* begin (or resume) from a persisted position */
	void		(*startup) (LogicalRepAssembly *self, const SinkCaps *caps,
							const ProgressPos *from);
	void		(*shutdown) (LogicalRepAssembly *self);

	/* read the sources for up to timeout_ms; false = the stream is gone */
	bool		(*poll) (LogicalRepAssembly *self, int timeout_ms);

	/* the next unit to apply, if one is complete and due; false = none yet */
	bool		(*next_unit) (LogicalRepAssembly *self, LogicalRepUnit *out);

	/* the rows of one transaction of that unit; only when delivers_rows */
	bool		(*next_change) (LogicalRepAssembly *self, const AssembledTxn *txn,
								LogicalChange *out);

	/* where this unit leaves the subscription, for the sink to persist */
	void		(*unit_progress) (LogicalRepAssembly *self, const LogicalRepUnit *unit,
								  ProgressPos *out);

	/* the unit is committed: release its buffers, send feedback, count it */
	void		(*advance) (LogicalRepAssembly *self, const LogicalRepUnit *unit);

	/*
	 * Between units: anything periodic that is not itself a unit.  Returns
	 * true if it has just made more units available and wants them applied
	 * before the driver reads the sources again -- that is what keeps a
	 * sequence that has to be atomic (Cloudberry: replay the held
	 * transactions of a finished copy, then publish the table) free of
	 * anything that arrives in between.
	 */
	bool		(*housekeeping) (LogicalRepAssembly *self);

	/*
	 * An error the loop caught, after both sides have been rolled back.
	 * Returns true to carry on (the driver rebuilds from the checkpoint and
	 * the stream replays), false to end the run -- which is the only honest
	 * answer to an error that will come back on every replay, such as a
	 * target the stream cannot be applied to at all.  The implementation
	 * decides how to tell whoever has to fix it.
	 */
	bool		(*handle_error) (LogicalRepAssembly *self, int sqlerrcode,
								 const char *message);

	/* drop everything not committed (after an error) ... */
	void		(*reset) (LogicalRepAssembly *self);
	/* ... and come back from the checkpoint the sink just handed back */
	void		(*restart) (LogicalRepAssembly *self, const ProgressPos *from);

	/* the loop should end (the subscription was disabled, a limit was hit) */
	bool		(*should_stop) (LogicalRepAssembly *self);

	/*
	 * Wait roughly this many milliseconds before the run is tried again.
	 * Called only from the recovery path, where there is nothing to do but
	 * back off.  It belongs to the assembly rather than the loop because how
	 * a wait is woken out of is a property of where the run lives: a worker
	 * has a latch its signal handler sets, and something that drives this
	 * loop outside a backend may have neither.  An implementation that
	 * cannot be woken should still return in about the time asked for.
	 */
	void		(*wait) (LogicalRepAssembly *self, int ms);

	void	   *private_data;
};

/*
 * The apply loop.  Capability-gated, commit-ordered, and recovering from
 * any error by rebuilding both sides from the persisted checkpoint.
 */
extern void lr_apply_driver_run(LogicalRepAssembly *assembly,
								LogicalRepSink *sink, void *sink_cfg);

/*
 * Implementations declare their own constructors, which take whatever that
 * implementation needs: the Cloudberry pair, for one, shares a target
 * session, because its reader gang and its writer gang have to live in one
 * session for the plans to dispatch.  See
 * mpp_apply_internal.h.
 */

#endif							/* LOGICALREP_SINK_H */
