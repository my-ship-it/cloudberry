/*-------------------------------------------------------------------------
 *
 * logicalrep_apply_driver_test.c
 *	  Unit tests for the apply loop.
 *
 * A mock assembly and a mock sink drive lr_apply_driver_run end to end, so
 * that the loop is exercised without a cluster on either side.  What is
 * checked is what the contract promises:
 *
 *	- every unit is begun, applied and committed once, in order
 *	- the capability bits gate the work: an out-of-band sink is handed whole
 *	  units and never a row; a row-delivering sink gets begin_txn and change
 *	  per transaction and never apply_unit
 *	- a relation enumeration happens only for a sink that asks for it
 *	- LR_COMMIT_RETRY aborts the unit and leaves it with the assembly
 *
 *-------------------------------------------------------------------------
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"

#include "utils/memutils.h"

/* the code under test */
#include "../logicalrep_apply_driver.c"

/* ------------------------------------------------------------------ *
 * Mock assembly: hands out nunits units of txns_per transactions
 * ------------------------------------------------------------------ */
typedef struct MockAsm
{
	int			nunits;			/* units left to hand out */
	int			txns_per;
	int			changes_per;
	int			nrels;			/* relations it puts in a unit, if asked */
	uint64		next_gxid;

	SinkCaps	caps;			/* what startup was told */
	int			cur_change;

	int			n_startup;
	int			n_poll;
	int			n_progress;
	int			n_advance;
	int			n_housekeeping;
	int			n_reset;
	int			n_restart;
	int			n_shutdown;
	int			n_handle_error;
	int			n_wait;
	bool		give_up;		/* what handle_error answers */
	int			fail_startups;	/* startups that throw before one succeeds */
	int			stop_after_startups;	/* 0 = never; else stop once this many
										 * startups have been attempted */
	int			fail_restarts;	/* restarts that throw before one succeeds */
	bool		connected;		/* a startup or restart has succeeded */

	/* storage for the unit handed out */
	AssembledTxn txns[8];
	LogicalRepRelLock rels[4];
} MockAsm;

static void
masm_startup(LogicalRepAssembly *self, const SinkCaps *caps, const ProgressPos *from)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	st->caps = *caps;
	st->n_startup++;
	if (st->fail_startups > 0)
	{
		st->fail_startups--;
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("mock assembly cannot come up yet")));
	}
	st->connected = true;
}

static void
masm_shutdown(LogicalRepAssembly *self)
{
	((MockAsm *) self->private_data)->n_shutdown++;
}

static bool
masm_poll(LogicalRepAssembly *self, int timeout_ms)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	st->n_poll++;
	/*
	 * A real assembly reads the source here.  Polling one that a failed
	 * restart left torn down is what used to crash the worker, so the mock
	 * insists on being connected.
	 */
	assert_true(st->connected);
	return true;
}

static bool
masm_next_unit(LogicalRepAssembly *self, LogicalRepUnit *out)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	if (st->nunits <= 0)
		return false;
	st->nunits--;

	memset(out, 0, sizeof(*out));
	out->ntxns = st->txns_per;
	out->txns = st->txns;
	for (int i = 0; i < st->txns_per; i++)
	{
		memset(&st->txns[i], 0, sizeof(AssembledTxn));
		st->txns[i].gxid = st->next_gxid++;
	}

	/*
	 * The enumeration is work the assembly only does when the sink asked
	 * for it; a sink that did not must see an empty set.
	 */
	if (st->caps.needs_relid_enumeration_for_lock)
	{
		out->nrels = st->nrels;
		out->rels = st->rels;
	}

	st->cur_change = st->changes_per;
	return true;
}

static bool
masm_next_change(LogicalRepAssembly *self, const AssembledTxn *txn, LogicalChange *out)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	if (st->cur_change <= 0)
	{
		st->cur_change = st->changes_per;	/* rearm for the next transaction */
		return false;
	}
	st->cur_change--;
	memset(out, 0, sizeof(*out));
	out->op = LR_CHANGE_INSERT;
	return true;
}

static void
masm_unit_progress(LogicalRepAssembly *self, const LogicalRepUnit *unit, ProgressPos *out)
{
	((MockAsm *) self->private_data)->n_progress++;
	memset(out, 0, sizeof(*out));
}

static void
masm_advance(LogicalRepAssembly *self, const LogicalRepUnit *unit)
{
	((MockAsm *) self->private_data)->n_advance++;
}

static bool
masm_housekeeping(LogicalRepAssembly *self)
{
	((MockAsm *) self->private_data)->n_housekeeping++;
	return false;				/* no extra units */
}

static void
masm_reset(LogicalRepAssembly *self)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	st->n_reset++;
	st->connected = false;		/* what it drops, restart has to rebuild */
}

static void
masm_restart(LogicalRepAssembly *self, const ProgressPos *from)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	st->n_restart++;
	if (st->fail_restarts > 0)
	{
		st->fail_restarts--;
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("mock assembly cannot reach the source yet")));
	}
	st->connected = true;
}

static bool
masm_handle_error(LogicalRepAssembly *self, int sqlerrcode, const char *message)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	st->n_handle_error++;
	return !st->give_up;
}

/* nothing to be woken out of here, and nothing to be gained by sleeping */
static void
masm_wait(LogicalRepAssembly *self, int ms)
{
	((MockAsm *) self->private_data)->n_wait++;
}

static bool
masm_should_stop(LogicalRepAssembly *self)
{
	MockAsm    *st = (MockAsm *) self->private_data;

	if (st->stop_after_startups > 0 && st->n_startup >= st->stop_after_startups)
		return true;
	return st->nunits <= 0;
}

/* ------------------------------------------------------------------ *
 * Mock sink: counts what the loop asked of it
 * ------------------------------------------------------------------ */
typedef struct MockSink
{
	SinkCaps	caps;
	CommitResult commit_result;
	int			retry_before;	/* ask for a retry on this many commits */
	int			fail_units;		/* raise an error on this many units */

	int			n_begin_unit;
	int			n_begin_txn;
	int			n_change;
	int			n_apply_unit;
	int			n_commit;
	int			n_abort;
	int			nrels_seen;
} MockSink;

static void msink_startup(LogicalRepSink *s, void *c) {}
static void msink_shutdown(LogicalRepSink *s) {}

static SinkCaps
msink_caps(LogicalRepSink *s)
{
	return ((MockSink *) s->private_data)->caps;
}

static ProgressPos
msink_ckpt(LogicalRepSink *s)
{
	ProgressPos p;

	memset(&p, 0, sizeof(p));
	return p;
}

static void
msink_begin_unit(LogicalRepSink *s, const LogicalRepUnit *u)
{
	MockSink   *st = (MockSink *) s->private_data;

	st->n_begin_unit++;
	st->nrels_seen += u->nrels;
}

static void
msink_begin_txn(LogicalRepSink *s, const AssembledTxn *t)
{
	((MockSink *) s->private_data)->n_begin_txn++;
}

static void
msink_change(LogicalRepSink *s, const LogicalChange *c)
{
	((MockSink *) s->private_data)->n_change++;
}

static void
msink_apply_unit(LogicalRepSink *s, const LogicalRepUnit *u)
{
	MockSink   *st = (MockSink *) s->private_data;

	st->n_apply_unit++;
	if (st->fail_units > 0)
	{
		st->fail_units--;
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("mock sink cannot apply this unit")));
	}
}

static CommitResult
msink_commit_unit(LogicalRepSink *s, const LogicalRepUnit *u, const ProgressPos *pos)
{
	MockSink   *st = (MockSink *) s->private_data;

	st->n_commit++;
	if (st->retry_before > 0)
	{
		st->retry_before--;
		return LR_COMMIT_RETRY;
	}
	return LR_COMMIT_OK;
}

static void
msink_abort(LogicalRepSink *s)
{
	((MockSink *) s->private_data)->n_abort++;
}

static LogicalRepAssembly
make_masm(MockAsm *st)
{
	LogicalRepAssembly a = {
		.startup = masm_startup,
		.shutdown = masm_shutdown,
		.poll = masm_poll,
		.next_unit = masm_next_unit,
		.next_change = masm_next_change,
		.unit_progress = masm_unit_progress,
		.advance = masm_advance,
		.housekeeping = masm_housekeeping,
		.handle_error = masm_handle_error,
		.reset = masm_reset,
		.restart = masm_restart,
		.should_stop = masm_should_stop,
		.wait = masm_wait,
		.private_data = st
	};

	return a;
}

static LogicalRepSink
make_msink(MockSink *st)
{
	LogicalRepSink s = {
		.startup = msink_startup,
		.shutdown = msink_shutdown,
		.capabilities = msink_caps,
		.get_checkpoint = msink_ckpt,
		.begin_unit = msink_begin_unit,
		.begin_txn = msink_begin_txn,
		.change = msink_change,
		.apply_unit = msink_apply_unit,
		.commit_unit = msink_commit_unit,
		.abort = msink_abort,
		.private_data = st
	};

	return s;
}

/* ------------------------------------------------------------------ *
 * Tests
 * ------------------------------------------------------------------ */

/*
 * The Cloudberry profile: units of several transactions, rows out of band,
 * relations enumerated for the coordinator to lock.
 */
static void
test_out_of_band_sink_applies_units(void **state)
{
	MockAsm		as = {.nunits = 3,.txns_per = 4,.changes_per = 2,.nrels = 2,.next_gxid = 100};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	ss.caps.commits_units = true;
	ss.caps.needs_participant_set = true;
	ss.caps.needs_relid_enumeration_for_lock = true;
	ss.caps.does_2pc = true;
	/* delivers_rows stays false: the rows do not pass through the driver */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	assert_int_equal(ss.n_begin_unit, 3);
	assert_int_equal(ss.n_apply_unit, 3);	/* one call per unit */
	assert_int_equal(ss.n_commit, 3);
	assert_int_equal(ss.n_begin_txn, 0);	/* never, for this profile */
	assert_int_equal(ss.n_change, 0);
	assert_int_equal(ss.nrels_seen, 6);	/* 2 relations in each unit */
	assert_int_equal(as.n_progress, 3);
	assert_int_equal(as.n_advance, 3);
	assert_int_equal(as.n_startup, 1);
	assert_int_equal(as.n_shutdown, 1);
}

/*
 * The single-node profile: one transaction per unit, rows delivered through
 * the driver, no enumeration and no locks.  The assembly is told as much at
 * startup and leaves the relation set empty, which is the point of the
 * capability bits: the work is not done, not merely ignored.
 */
static void
test_row_delivering_sink_gets_rows(void **state)
{
	MockAsm		as = {.nunits = 2,.txns_per = 1,.changes_per = 3,.nrels = 2,.next_gxid = 1};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	ss.caps.delivers_rows = true;	/* everything else off */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	assert_int_equal(ss.n_begin_unit, 2);
	assert_int_equal(ss.n_begin_txn, 2);	/* one transaction per unit */
	assert_int_equal(ss.n_change, 6);	/* three rows each */
	assert_int_equal(ss.n_apply_unit, 0);	/* never, for this profile */
	assert_int_equal(ss.n_commit, 2);
	assert_int_equal(ss.nrels_seen, 0);	/* not enumerated, not asked for */
	assert_int_equal(as.n_advance, 2);
	assert_false(as.caps.needs_relid_enumeration_for_lock);
}

/*
 * A transient commit failure aborts the unit and does not advance.  (The
 * mock does not re-offer the unit; what is under test is that the driver
 * rolls it back and leaves the progress where it was.)
 */
static void
test_commit_retry_does_not_advance(void **state)
{
	MockAsm		as = {.nunits = 2,.txns_per = 1,.changes_per = 1,.next_gxid = 7};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	ss.caps.commits_units = true;
	ss.retry_before = 1;		/* the first commit asks for a retry */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	assert_int_equal(ss.n_commit, 2);	/* both units reached commit */
	assert_int_equal(ss.n_abort, 1);	/* the retried one was rolled back */
	assert_int_equal(as.n_advance, 1);	/* only the committed one advanced */
}

/* Nothing to apply: the loop starts, polls nothing and stops cleanly. */
static void
test_empty_stream(void **state)
{
	MockAsm		as = {.nunits = 0};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly = make_masm(&as);
	LogicalRepSink sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	assert_int_equal(ss.n_begin_unit, 0);
	assert_int_equal(ss.n_commit, 0);
	assert_int_equal(as.n_startup, 1);
	assert_int_equal(as.n_shutdown, 1);
}

/*
 * An error the assembly says will come back ends the run instead of being
 * replayed for ever.
 */
static void
test_permanent_error_ends_the_run(void **state)
{
	MockAsm		as = {.nunits = 5,.txns_per = 1,.changes_per = 1,.next_gxid = 1};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	as.give_up = true;			/* "this will not pass" */
	ss.caps.commits_units = true;
	ss.fail_units = 99;			/* every unit fails */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	assert_int_equal(as.n_handle_error, 1);	/* asked once, answered no */
	assert_int_equal(as.n_restart, 0);	/* and never came back */
	assert_int_equal(ss.n_abort, 1);
	assert_int_equal(as.n_reset, 1);
	assert_int_equal(ss.n_commit, 0);
	assert_int_equal(as.n_shutdown, 1);	/* still shut down cleanly */
	assert_int_equal(as.nunits, 4);	/* stopped at the first unit */
}

/* an error it says can pass is replayed from the checkpoint */
static void
test_a_failed_startup_is_graded_not_escaped(void **state)
{
	MockAsm		as = {.nunits = 2,.txns_per = 1,.changes_per = 1,.next_gxid = 1};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	as.give_up = false;			/* "try again" */
	ss.caps.commits_units = true;
	as.fail_startups = 2;		/* the first two attempts cannot come up */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	/*
	 * Coming up is part of the loop, so failing at it is an error the loop
	 * grades and retries -- not something that escapes the run and leaves the
	 * worker to be started again by someone else, which is how it used to be
	 * and why a subscription whose slot had been invalidated span for ever.
	 */
	assert_int_equal(as.n_startup, 3);
	assert_int_equal(as.n_handle_error, 2);
	assert_int_equal(ss.n_commit, 2);	/* and then it did its work */
	assert_int_equal(as.n_shutdown, 1);
}

/* and a stop asked for while it still cannot come up is a stop */
static void
test_a_stop_while_it_cannot_come_up_is_honoured(void **state)
{
	MockAsm		as = {.nunits = 2,.txns_per = 1,.changes_per = 1,.next_gxid = 1};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	as.give_up = false;			/* "try again" */
	ss.caps.commits_units = true;
	as.fail_startups = 5;		/* it would keep failing */
	as.stop_after_startups = 2; /* but is told to stop after two attempts */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	/*
	 * Coming up is retried in a loop of its own, and the loop used to ask
	 * whether to stop only once the run was up -- so a run that could not
	 * come up never asked, and a worker whose source had gone retried there
	 * for as long as the process lived, deaf to the stop that had been asked
	 * for.  It is asked first now, before coming up is attempted again.
	 */
	assert_int_equal(as.n_startup, 2);
	assert_int_equal(as.n_handle_error, 2);
	assert_int_equal(as.n_wait, 2);		/* it backed off between attempts */
	assert_int_equal(ss.n_commit, 0);	/* it never got to do any work */
	assert_int_equal(as.n_shutdown, 1); /* and was still shut down once */
}

static void
test_a_failed_restart_is_retried_not_polled_through(void **state)
{
	MockAsm		as = {.nunits = 3,.txns_per = 1,.changes_per = 1,.next_gxid = 1};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	as.give_up = false;
	ss.caps.commits_units = true;
	ss.fail_units = 1;			/* one error, to send it into recovery */
	as.fail_restarts = 2;		/* and the first two restarts fail too */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	/*
	 * Three restarts: two that threw and the one that worked.  The run has to
	 * come out of it having applied everything -- and masm_poll asserts it was
	 * never called on a torn-down assembly, which is the crash this guards.
	 */
	assert_int_equal(as.n_restart, 3);
	assert_int_equal(as.n_handle_error, 3);
	assert_int_equal(ss.n_commit, 2);	/* the unit that failed is gone, the rest applied */
	assert_int_equal(as.n_shutdown, 1);
}

static void
test_transient_error_is_retried(void **state)
{
	MockAsm		as = {.nunits = 3,.txns_per = 1,.changes_per = 1,.next_gxid = 1};
	MockSink	ss = {{0}};
	LogicalRepAssembly assembly;
	LogicalRepSink sink;

	as.give_up = false;			/* "try again" */
	ss.caps.commits_units = true;
	ss.fail_units = 1;			/* only the first one */

	assembly = make_masm(&as);
	sink = make_msink(&ss);

	lr_apply_driver_run(&assembly, &sink, NULL);

	assert_int_equal(as.n_handle_error, 1);
	assert_int_equal(as.n_restart, 1);	/* rebuilt from the checkpoint */
	assert_int_equal(ss.n_commit, 2);	/* and applied what was left */
	assert_int_equal(as.n_shutdown, 1);
}

int
main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	/* the error paths raise real errors, which needs ErrorContext */
	MemoryContextInit();

	const		UnitTest tests[] = {
		unit_test(test_out_of_band_sink_applies_units),
		unit_test(test_row_delivering_sink_gets_rows),
		unit_test(test_commit_retry_does_not_advance),
		unit_test(test_permanent_error_ends_the_run),
		unit_test(test_transient_error_is_retried),
		unit_test(test_a_failed_restart_is_retried_not_polled_through),
		unit_test(test_a_failed_startup_is_graded_not_escaped),
		unit_test(test_a_stop_while_it_cannot_come_up_is_honoured),
		unit_test(test_empty_stream)
	};

	return run_tests(tests);
}
