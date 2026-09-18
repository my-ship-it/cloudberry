/*-------------------------------------------------------------------------
 *
 * logicalrep_apply_driver.c
 *	  The apply loop of logical replication, for any target.
 *
 * It depends on LogicalRepAssembly and LogicalRepSink and on nothing else:
 * not on Cloudberry, not on the shape of the source stream.  Everything
 * specific to a target is behind the sink, everything specific to a stream
 * is behind the assembly, and the capability bits the sink reports decide
 * how much work either side does.
 *
 * The loop is the one Cloudberry's own apply was built around, generalised:
 * poll the sources, apply whatever units came due, do the periodic work, and
 * on any error roll back and rebuild both sides from the progress the target
 * has committed.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/logicalrep_apply_driver.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"
#include "logicalrep_sink.h"

/* the longest one poll may wait; the assembly may choose to wait less */
#define LR_DRIVER_POLL_MS		1000

/* how long to wait before rebuilding after an error, and after a failed one */
#define LR_DRIVER_RECOVER_MS	500
#define LR_DRIVER_RETRY_MS		2000

static bool apply_one_unit(LogicalRepAssembly *assembly, LogicalRepSink *sink,
						   const SinkCaps *caps, const LogicalRepUnit *unit);

/*
 * Run until the assembly says to stop.
 *
 * Errors are contained here: any of them ends the open target transaction,
 * throws away everything not committed and resumes from the checkpoint.
 * That is safe because a unit is applied and its progress persisted in one
 * target transaction, so a replay either finds the unit applied (and the
 * assembly drops it) or finds no trace of it.
 */
void
lr_apply_driver_run(LogicalRepAssembly *assembly, LogicalRepSink *sink,
					void *sink_cfg)
{
	SinkCaps	caps;
	ProgressPos pos;
	MemoryContext cxt = CurrentMemoryContext;
	bool		give_up = false;
	bool		needs_startup = true;
	bool		needs_restart = false;

	sink->startup(sink, sink_cfg);
	caps = sink->capabilities(sink);	/* once; it decides the whole shape */
	pos = sink->get_checkpoint(sink);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Whether to stop is the assembly's to say, but only once it is up:
		 * the run has always come up before anything else, and shutdown is
		 * paired with startup.  A stop asked for while it cannot come up is
		 * honoured as soon as it does, or when the error grading below gives
		 * up on it; a signal is handled by CHECK_FOR_INTERRUPTS above either
		 * way.
		 */
		if (give_up || (!needs_startup && assembly->should_stop(assembly)))
			break;

		PG_TRY();
		{
			LogicalRepUnit unit;

			/*
			 * Starting and resuming are both done here, at the top of a pass,
			 * rather than before the loop or at the end of the recovery that
			 * called for them.  That way an assembly that fails to come up --
			 * the source is down, a slot it needs cannot be streamed from --
			 * is an error of this loop like any other, and is graded like any
			 * other; before, it escaped the loop entirely and the worker
			 * merely exited, to be started again with the same result for as
			 * long as anyone was watching.  It also keeps a failed resume
			 * from leaving the assembly half torn down for the next poll to
			 * read a source from that is no longer connected.
			 */
			if (needs_startup)
			{
				assembly->startup(assembly, &caps, &pos);
				needs_startup = false;
			}
			else if (needs_restart)
			{
				pos = sink->get_checkpoint(sink);
				assembly->restart(assembly, &pos);
				needs_restart = false;
			}

			/* the sources */
			if (!assembly->poll(assembly, LR_DRIVER_POLL_MS))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("logical replication apply: lost the source stream")));

			/*
			 * Whatever came due, in commit order, and then the periodic
			 * work -- which may itself make more units due, in which case
			 * they are applied before the sources are read again.  The
			 * assembly decides what a unit contains and when one is
			 * complete; it hands out the next only after the previous one
			 * is committed, so a sequence it needs to be atomic stays free
			 * of anything that arrives meanwhile.
			 */
			for (;;)
			{
				bool		stop = false;

				while (assembly->next_unit(assembly, &unit))
				{
					if (!apply_one_unit(assembly, sink, &caps, &unit))
					{
						stop = true;	/* transient: poll again, retry it */
						break;
					}

					CHECK_FOR_INTERRUPTS();
					if (assembly->should_stop(assembly))
					{
						stop = true;
						break;
					}
				}

				if (stop || !assembly->housekeeping(assembly))
					break;

				CHECK_FOR_INTERRUPTS();
			}
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
			elog(LOG, "logical replication apply: recovering after: [%s] %s",
				 unpack_sql_state(sqlerrcode), message);
			FreeErrorData(edata);

			sink->abort(sink);
			assembly->reset(assembly);

			/*
			 * Not every error is worth replaying into.  The assembly decides;
			 * when it says no, the run ends here rather than failing the same
			 * way for ever.
			 */
			if (!assembly->handle_error(assembly, sqlerrcode, message))
				give_up = true;
			else
			{
				pg_usleep((needs_startup || needs_restart ? LR_DRIVER_RETRY_MS
						   : LR_DRIVER_RECOVER_MS) * 1000L);
				if (!needs_startup)
					needs_restart = true;
			}
		}
		PG_END_TRY();

		/* PG_CATCH() sits inside a do/while, so the loop is left out here */
		if (give_up)
			break;
	}

	assembly->shutdown(assembly);
	sink->shutdown(sink);
}

/*
 * One unit: open the target transaction, put the changes in it, persist the
 * progress and commit.  Returns false if the target asked for a retry, in
 * which case nothing of the unit is committed and the assembly still holds
 * it.
 */
static bool
apply_one_unit(LogicalRepAssembly *assembly, LogicalRepSink *sink,
			   const SinkCaps *caps, const LogicalRepUnit *unit)
{
	ProgressPos pos;
	CommitResult r;

	/* open, and take the locks the unit needs (only if the sink asked) */
	sink->begin_unit(sink, unit);

	if (caps->delivers_rows)
	{
		/* the rows come through this process, transaction by transaction */
		for (int i = 0; i < unit->ntxns; i++)
		{
			const AssembledTxn *txn = &unit->txns[i];
			LogicalChange chg;

			sink->begin_txn(sink, txn);
			while (assembly->next_change(assembly, txn, &chg))
				sink->change(sink, &chg);
		}
	}
	else
	{
		/* the rows never touch this process: the sink's transport moves them */
		sink->apply_unit(sink, unit);
	}

	/* the position this unit leaves behind, committed with it */
	assembly->unit_progress(assembly, unit, &pos);

	r = sink->commit_unit(sink, unit, &pos);

	if (r == LR_COMMIT_RETRY)
	{
		sink->abort(sink);
		return false;
	}
	if (r == LR_COMMIT_FATAL)
		ereport(ERROR,
				(errmsg("logical replication apply failed for a unit of %d transaction(s) at %X/%X",
						unit->ntxns, LSN_FORMAT_ARGS(unit->commit_order_pos))));

	assembly->advance(assembly, unit);
	return true;
}
