/*-------------------------------------------------------------------------
 *
 * mpp_reader.c
 *	  Reader state of MPP logical replication apply (see mpp_reader.h).
 *
 * The reader is the receiving end of one source segment's pgoutput stream.
 * It does not apply anything: it buffers each transaction's changes until
 * the coordinator, driven by the source coordinator's commit events, asks
 * for them.  Reading the socket happens only inside LrReaderReceive(), so
 * when nobody asks the TCP window fills up and the source walsender blocks:
 * that is the backpressure of the design.
 *
 * Message handling follows the stock apply worker's dispatch (worker.c),
 * with two differences: nothing is applied, and the streaming protocol's
 * per-message xid is stripped so that stored bodies are in the plain form
 * the writer consumes.  Sub-transaction aborts of streamed transactions
 * truncate the change log like worker.c's stream_abort_internal().
 *
 * Memory: everything lives in a context under TopMemoryContext so that the
 * state survives statements and transactions of the reader QE.  Changes are
 * kept in memory up to gp_lr_reader_mem; beyond that the largest
 * transactions are spilled to inter-transaction temporary files.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_reader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "nodes/value.h"
#include "pgstat.h"
#include "replication/logicalproto.h"
#include "mpp_reader.h"
#include "replication/walreceiver.h"
#include "storage/buffile.h"
#include "storage/latch.h"
#include "storage/lmgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

int			gp_lr_reader_mem = 65536;	/* kB */

#define LR_GXID_MESSAGE_PREFIX	"cbdb.gxid"
#define LR_MESSAGE_TRANSACTIONAL (1 << 0)	/* proto.c's MESSAGE_TRANSACTIONAL */
#define LR_FEEDBACK_INTERVAL_MS	1000

/*
 * Change log of one transaction: a sequence of records
 *   uint8 op, uint32 relid, uint32 len, char body[len]
 * held in memory until spilled, then in a temporary file.  total_len is the
 * logical length; on a spilled log the file may hold garbage beyond it
 * after a sub-transaction abort truncated the log.
 */
typedef struct LrChangeLog
{
	char	   *mem;
	Size		mem_alloc;
	BufFile    *file;
	bool		spilled;
	Size		total_len;
} LrChangeLog;

typedef struct LrRecordHeader
{
	uint8		op;
	uint32		relid;
	uint32		len;
} LrRecordHeader;

/* first change of a sub-transaction in a streamed transaction */
typedef struct LrSubxact
{
	TransactionId subxid;
	Size		offset;
	int64		nchanges;
	int64		nbytes;
} LrSubxact;

/* a relation described by the stream */
typedef struct LrRel
{
	Oid			relid;			/* hash key */
	char	   *raw;			/* R message body (no type byte) */
	int			rawlen;
	LogicalRepRelation *rel;	/* parsed copy, owned by the state */
	uint32		generation;		/* bumped on every new description */
} LrRel;

typedef struct LrGxidEntry
{
	int64		gxid;			/* hash key */
	LrTxn	   *txn;
} LrGxidEntry;

typedef struct LrAppliedEntry
{
	int64		gxid;			/* hash key */
} LrAppliedEntry;

struct LrReaderState
{
	Oid			subid;
	int64		token;
	MemoryContext cxt;			/* long-lived */
	MemoryContext msgcxt;		/* scratch, reset per message */

	/* connection */
	WalReceiverConn *conn;
	char	   *conninfo;
	char	   *slotname;
	bool		eof;

	/* transactions */
	HTAB	   *xids;			/* xid -> LrTxn */
	HTAB	   *gxids;			/* gxid -> LrGxidEntry */
	HTAB	   *rels;			/* relid -> LrRel */
	HTAB	   *applied;		/* replay dedup, gxid -> LrAppliedEntry */
	XLogRecPtr	start_lsn;		/* where to resume the stream: the driver's
								 * persisted seg_flush.  The
								 * slot's confirmed_flush only advances with
								 * our feedback and can be far behind what has
								 * already been applied, so resuming there
								 * would replay applied transactions that the
								 * applied-gxid set no longer covers (it is
								 * trimmed at seg_flush). */
	XLogRecPtr	replay_seg_pos; /* replay dedup for local transactions */

	/* the cut point's snapshot of this source segment (LrReaderStartPoint) */
	TransactionId sync_xmin;
	TransactionId sync_xmax;
	int			nsync_xip;
	TransactionId *sync_xip;
	List	   *pending;		/* LrTxn*: ready, not yet reported */
	List	   *local_ready;	/* LrTxn*: local, committed, in stream order */

	/* decoding position */
	TransactionId cur_xid;		/* non-streamed transaction in progress */
	TransactionId stream_xid;	/* streamed transaction whose block is open */
	bool		in_stream;
	XLogRecPtr	pos;			/* WAL end of the last message or keepalive */
	XLogRecPtr	last_feedback_flush;
	TimestampTz last_feedback_time;

	/*
	 * The source segment this reader streams from, or -1 when the source has
	 * fewer segments than the target and this reader has no source at all.
	 * Reader index (= target segment) and source content id are two
	 * different coordinate systems; everything that talks
	 * to the coordinator uses the reader index, everything that talks about
	 * the stream uses this.
	 */
	int			source_seg;

	/*
	 * Source relations that are DISTRIBUTED REPLICATED: every source segment
	 * holds every row, so every segment stream carries their changes.  Only
	 * one reader may emit those (mpp_source.c), or the target would apply
	 * each row once per source segment.  Set from the CONNECT spec.
	 */
	List	   *replicated_rels;	/* Oid */

	/* accounting */
	Size		mem_bytes;
	Size		spill_bytes;
	int			nspilled;
	int64		nmsgs;
};

/* process-wide registry: subid -> state */
static List *lr_readers = NIL;

static LrTxn *txn_lookup(LrReaderState *st, TransactionId xid, bool create);
static void txn_drop(LrReaderState *st, LrTxn *txn);
static void txn_finish(LrReaderState *st, LrTxn *txn, LrTxnState state,
					   XLogRecPtr commit_lsn, XLogRecPtr end_lsn);
static void txn_set_gxid(LrReaderState *st, LrTxn *txn, int64 gxid,
						 LrTxnKind kind);
static bool txn_is_ready(const LrTxn *txn);
static void txn_append(LrReaderState *st, LrTxn *txn, char op, Oid relid,
					   const char *body, int len);
static void txn_describe_rel(LrReaderState *st, LrTxn *txn, Oid relid);
static void txn_note_subxact(LrReaderState *st, LrTxn *txn,
							 TransactionId subxid);
static void txn_abort_subxact(LrReaderState *st, LrTxn *txn,
							  TransactionId subxid);
static bool parse_gid(const char *gid, int64 *gxid);
static bool txn_covered_by_sync(LrReaderState *st, TransactionId xid);
static void maybe_spill(LrReaderState *st);
static void changelog_init(LrChangeLog *log);
static void changelog_append(LrReaderState *st, LrChangeLog *log,
							 const void *data, Size len);
static void changelog_spill(LrReaderState *st, LrChangeLog *log);
static void changelog_truncate(LrReaderState *st, LrChangeLog *log, Size len);
static void changelog_free(LrReaderState *st, LrChangeLog *log);
static void handle_relation(LrReaderState *st, StringInfo s);
static LrTxn *current_txn(LrReaderState *st, TransactionId subxid,
						  const char *what);

/* ------------------------------------------------------------------------
 * Lifetime
 * ------------------------------------------------------------------------
 */

LrReaderState *
LrReaderCreate(Oid subid, int srcseg, int64 token, const LrReaderStartPoint *start)
{
	MemoryContext cxt;
	MemoryContext oldcxt;
	LrReaderState *st;
	HASHCTL		ctl;

	if (LrReaderFind(subid, srcseg) != NULL)
		elog(ERROR, "a reader state for subscription %u, source segment %d already exists",
			 subid, srcseg);

	cxt = AllocSetContextCreate(TopMemoryContext, "LrReaderState",
								ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(cxt);

	st = palloc0(sizeof(LrReaderState));
	st->subid = subid;
	st->source_seg = srcseg;
	st->token = token;
	st->cxt = cxt;
	st->msgcxt = AllocSetContextCreate(cxt, "LrReaderState message",
									   ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(LrTxn);
	ctl.hcxt = cxt;
	st->xids = hash_create("LrReader xids", 256, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ctl.keysize = sizeof(int64);
	ctl.entrysize = sizeof(LrGxidEntry);
	st->gxids = hash_create("LrReader gxids", 256, &ctl,
							HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(LrRel);
	st->rels = hash_create("LrReader relations", 64, &ctl,
						   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	ctl.keysize = sizeof(int64);
	ctl.entrysize = sizeof(LrAppliedEntry);
	st->applied = hash_create("LrReader applied gxids", 256, &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	if (start)
	{
		st->start_lsn = start->seg_flush;
		st->replay_seg_pos = start->seg_pos;
		st->sync_xmin = start->sync_xmin;
		st->sync_xmax = start->sync_xmax;
		st->nsync_xip = start->nsync_xip;
		if (start->nsync_xip > 0)
		{
			st->sync_xip = palloc(sizeof(TransactionId) * start->nsync_xip);
			memcpy(st->sync_xip, start->sync_xip,
				   sizeof(TransactionId) * start->nsync_xip);
		}
		for (int i = 0; i < start->napplied; i++)
			hash_search(st->applied, &start->applied[i], HASH_ENTER, NULL);
	}

	MemoryContextSwitchTo(TopMemoryContext);
	lr_readers = lappend(lr_readers, st);
	MemoryContextSwitchTo(oldcxt);

	return st;
}

/*
 * The state of one stream: a subscription and the source segment it reads.
 *
 * One reader QE holds as many of these as the driver gave it source segments
 * -- more than one when the source has more segments than the target.  A
 * reader with no source at all holds one with srcseg = -1,
 * so that the plans still find it warm.
 */
LrReaderState *
LrReaderFind(Oid subid, int srcseg)
{
	ListCell   *lc;

	foreach(lc, lr_readers)
	{
		LrReaderState *st = (LrReaderState *) lfirst(lc);

		if (st->subid == subid && st->source_seg == srcseg)
			return st;
	}
	return NULL;
}

/* every stream of this subscription, in the order they were created */
List *
LrReaderListSub(Oid subid)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, lr_readers)
	{
		LrReaderState *st = (LrReaderState *) lfirst(lc);

		if (st->subid == subid)
			result = lappend(result, st);
	}
	return result;
}

/* forget every stream of this subscription (a reconnect) */
void
LrReaderDestroySub(Oid subid)
{
	List	   *all = LrReaderListSub(subid);
	ListCell   *lc;

	foreach(lc, all)
		LrReaderDestroy((LrReaderState *) lfirst(lc));
	list_free(all);
}

void
LrReaderDestroy(LrReaderState *st)
{
	HASH_SEQ_STATUS seq;
	LrTxn	   *txn;

	lr_readers = list_delete_ptr(lr_readers, st);

	if (st->conn)
	{
		walrcv_disconnect(st->conn);
		st->conn = NULL;
	}

	/* spill files are not freed by the memory context */
	hash_seq_init(&seq, st->xids);
	while ((txn = (LrTxn *) hash_seq_search(&seq)) != NULL)
	{
		if (txn->changes)
			changelog_free(st, txn->changes);
	}

	MemoryContextDelete(st->cxt);
}

Oid
LrReaderSubid(LrReaderState *st)
{
	return st->subid;
}

int64
LrReaderToken(LrReaderState *st)
{
	return st->token;
}

/* ------------------------------------------------------------------------
 * Stream
 * ------------------------------------------------------------------------
 */

void
LrReaderConnect(LrReaderState *st, const char *conninfo, const char *slotname,
				List *publications, bool binary, bool streaming, bool twophase)
{
	MemoryContext oldcxt;
	WalRcvStreamOptions opts;
	char	   *err = NULL;
	ListCell   *lc;

	if (st->conn)
		elog(ERROR, "reader for subscription %u is already connected", st->subid);

	/* libpqwalreceiver is linked into the backend (subscriptioncmds.c does this) */
	if (WalReceiverFunctions == NULL)
		libpqwalreceiver_PG_init();

	oldcxt = MemoryContextSwitchTo(st->cxt);

	st->conninfo = pstrdup(conninfo);
	st->slotname = pstrdup(slotname);
	st->conn = walrcv_connect(conninfo, true, false, "mpp_apply_reader", &err);
	if (st->conn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not connect to the source segment: %s",
						err ? err : "unknown error")));

	memset(&opts, 0, sizeof(opts));
	opts.logical = true;
	opts.slotname = st->slotname;
	/*
	 * Resume exactly where the coordinator's progress says;
	 * only a reader that has no progress at all falls back to the slot's
	 * confirmed_flush.
	 */
	opts.startpoint = st->start_lsn;
	opts.proto.logical.proto_version = LOGICALREP_PROTO_MAX_VERSION_NUM;
	opts.proto.logical.publication_names = NIL;
	foreach(lc, publications)
		opts.proto.logical.publication_names =
			lappend(opts.proto.logical.publication_names,
					makeString(pstrdup(strVal(lfirst(lc)))));
	opts.proto.logical.binary = binary;
	opts.proto.logical.streaming_str = streaming ? "on" : NULL;
	opts.proto.logical.twophase = twophase;
	opts.proto.logical.origin = NULL;
	/* one-phase commits announce their gxid (pgoutput's cbdb_gxid option) */
	opts.proto.logical.extra_options = "cbdb_gxid 'on'";

	if (!walrcv_startstreaming(st->conn, &opts))
		ereport(ERROR,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not start streaming from slot \"%s\"", slotname)));

	MemoryContextSwitchTo(oldcxt);

	st->eof = false;
	st->last_feedback_time = GetCurrentTimestamp();
}

bool
LrReaderIsConnected(LrReaderState *st)
{
	return st->conn != NULL && !st->eof;
}

static bool
target_reached(LrReaderState *st, int64 target_gxid)
{
	LrGxidStatus s;

	if (target_gxid == 0)
		return false;
	s = LrReaderGxidStatus(st, target_gxid);
	return s == LR_GXID_READY || s == LR_GXID_APPLIED;
}

/*
 * Read from the socket.  Stops when the target gxid is ready (or, with
 * stop_on_any_ready, when any transaction became ready), when the socket
 * has nothing more (wait_ms == 0), when wait_ms elapsed, or when the
 * connection is gone.  Everything read is processed, whatever the reason.
 *
 * Once the condition holds we still take what is already buffered on the
 * socket, without waiting and up to a bounded number of messages, so that
 * one DRAIN reports as much as is there rather than the first transaction
 * alone.
 */
#define LR_RECEIVE_TAIL_MESSAGES	4096

LrReceiveResult
LrReaderReceive(LrReaderState *st, int64 target_gxid, int wait_ms,
				bool stop_on_any_ready)
{
	TimestampTz deadline = 0;
	bool		satisfied = false;
	int			tail = 0;

	if (st->conn == NULL || st->eof)
		return LR_RECV_EOF;
	if (wait_ms > 0)
		deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(), wait_ms);

	for (;;)
	{
		char	   *buf;
		int			len;
		pgsocket	fd = PGINVALID_SOCKET;
		long		remain;

		CHECK_FOR_INTERRUPTS();

		if (satisfied && tail++ >= LR_RECEIVE_TAIL_MESSAGES)
			return LR_RECV_TARGET_READY;

		len = walrcv_receive(st->conn, &buf, &fd);
		if (len < 0)
		{
			st->eof = true;
			return LR_RECV_EOF;
		}
		if (len > 0)
		{
			StringInfoData s;

			s.data = buf;
			s.len = len;
			s.maxlen = -1;
			s.cursor = 0;

			switch (pq_getmsgbyte(&s))
			{
				case 'w':
					{
						XLogRecPtr	start pg_attribute_unused();
						XLogRecPtr	walend;

						start = pq_getmsgint64(&s);
						walend = pq_getmsgint64(&s);
						(void) pq_getmsgint64(&s);	/* send time */
						LrReaderProcessMessage(st, s.data + s.cursor,
											   s.len - s.cursor, walend);
						break;
					}
				case 'k':
					{
						XLogRecPtr	walend = pq_getmsgint64(&s);
						bool		reply;

						(void) pq_getmsgint64(&s);	/* send time */
						reply = pq_getmsgbyte(&s);
						if (walend > st->pos)
							st->pos = walend;
						if (reply)
							LrReaderSendFeedback(st);
						break;
					}
				default:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("invalid message type \"%c\" from the source segment",
									buf[0])));
			}

			if (target_reached(st, target_gxid) ||
				(stop_on_any_ready && st->pending != NIL))
				satisfied = true;
			continue;			/* take everything that is buffered */
		}

		/* nothing buffered */
		if (satisfied || target_reached(st, target_gxid) ||
			(stop_on_any_ready && st->pending != NIL))
			return LR_RECV_TARGET_READY;
		if (wait_ms == 0)
			return LR_RECV_DRAINED;
		remain = TimestampDifferenceMilliseconds(GetCurrentTimestamp(), deadline);
		if (remain <= 0)
			return LR_RECV_TIMEOUT;

		(void) WaitLatchOrSocket(MyLatch,
								 WL_LATCH_SET | WL_SOCKET_READABLE | WL_TIMEOUT |
								 WL_EXIT_ON_PM_DEATH,
								 fd, remain, WAIT_EVENT_LOGICAL_APPLY_MAIN);
		ResetLatch(MyLatch);
	}
}

/*
 * Standby status update.  write = what we received, flush = apply = the
 * position below which everything is applied (LrReaderFlushPosition); the
 * walsender takes confirmed_flush from the flush field.
 */
void
LrReaderSendFeedback(LrReaderState *st)
{
	StringInfoData buf;
	XLogRecPtr	flush;

	if (st->conn == NULL || st->eof || XLogRecPtrIsInvalid(st->pos))
		return;

	flush = LrReaderFlushPosition(st);

	initStringInfo(&buf);
	pq_sendbyte(&buf, 'r');
	pq_sendint64(&buf, st->pos);	/* write */
	pq_sendint64(&buf, flush);	/* flush */
	pq_sendint64(&buf, flush);	/* apply */
	pq_sendint64(&buf, GetCurrentTimestamp());
	pq_sendbyte(&buf, 0);		/* no reply requested */
	walrcv_send(st->conn, buf.data, buf.len);
	pfree(buf.data);

	st->last_feedback_flush = flush;
	st->last_feedback_time = GetCurrentTimestamp();
}

/*
 * The largest position such that every transaction below it has been
 * applied and released: the smallest commit/prepare position among the
 * buffered completed transactions, else the read position.  Restarting the
 * stream there re-delivers exactly the still-buffered transactions
 * (decoding skips a transaction only when its commit record is below the
 * start point).
 */
XLogRecPtr
LrReaderFlushPosition(LrReaderState *st)
{
	HASH_SEQ_STATUS seq;
	LrTxn	   *txn;
	XLogRecPtr	flush = st->pos;

	hash_seq_init(&seq, st->xids);
	while ((txn = (LrTxn *) hash_seq_search(&seq)) != NULL)
	{
		if (txn->state == LR_TXN_OPEN)
			continue;
		if (XLogRecPtrIsInvalid(flush) || txn->commit_lsn < flush)
			flush = txn->commit_lsn;
	}
	return flush;
}

XLogRecPtr
LrReaderReadPosition(LrReaderState *st)
{
	return st->pos;
}

/* ------------------------------------------------------------------------
 * Message processing
 * ------------------------------------------------------------------------
 */

/*
 * Process one logical replication message (type byte included), as it
 * arrived in a 'w' message whose WAL end is walend.
 */
void
LrReaderProcessMessage(LrReaderState *st, const char *data, int len,
					   XLogRecPtr walend)
{
	StringInfoData s;
	MemoryContext oldcxt;
	char		type;
	TransactionId subxid = InvalidTransactionId;

	if (walend > st->pos)
		st->pos = walend;
	st->nmsgs++;

	MemoryContextReset(st->msgcxt);
	oldcxt = MemoryContextSwitchTo(st->msgcxt);

	/*
	 * A private, NUL-terminated copy: pq_getmsgstring() relies on the
	 * terminator and the walreceiver buffer is not ours to keep.
	 */
	s.data = palloc(len + 1);
	memcpy(s.data, data, len);
	s.data[len] = '\0';
	s.len = len;
	s.maxlen = len + 1;
	s.cursor = 0;

	type = pq_getmsgbyte(&s);

	/*
	 * Inside a stream block every change-level message carries the xid of
	 * the (sub)transaction after the type byte.  Strip it: stored bodies
	 * are in the non-streaming form.
	 */
	if (st->in_stream)
	{
		switch (type)
		{
			case LOGICAL_REP_MSG_INSERT:
			case LOGICAL_REP_MSG_UPDATE:
			case LOGICAL_REP_MSG_DELETE:
			case LOGICAL_REP_MSG_TRUNCATE:
			case LOGICAL_REP_MSG_RELATION:
			case LOGICAL_REP_MSG_TYPE:
			case LOGICAL_REP_MSG_MESSAGE:
				subxid = pq_getmsgint(&s, 4);
				if (!TransactionIdIsValid(subxid))
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("invalid transaction ID in streamed replication transaction")));
				break;
			default:
				break;
		}
	}

	switch (type)
	{
		case LOGICAL_REP_MSG_BEGIN:
			{
				LogicalRepBeginData begin;
				LrTxn	   *txn;

				logicalrep_read_begin(&s, &begin);
				txn = txn_lookup(st, begin.xid, true);
				if (XLogRecPtrIsInvalid(txn->first_lsn))
					txn->first_lsn = walend;
				st->cur_xid = begin.xid;
				break;
			}

		case LOGICAL_REP_MSG_BEGIN_PREPARE:
			{
				LogicalRepPreparedTxnData begin;
				LrTxn	   *txn;
				int64		gxid;

				logicalrep_read_begin_prepare(&s, &begin);
				txn = txn_lookup(st, begin.xid, true);
				if (XLogRecPtrIsInvalid(txn->first_lsn))
					txn->first_lsn = walend;
				if (parse_gid(begin.gid, &gxid))
					txn_set_gxid(st, txn, gxid, LR_TXN_TWOPHASE);
				else
					txn->kind = LR_TXN_LOCAL;	/* a user's PREPARE TRANSACTION */
				st->cur_xid = begin.xid;
				break;
			}

		case LOGICAL_REP_MSG_COMMIT:
			{
				LogicalRepCommitData commit;
				LrTxn	   *txn = current_txn(st, InvalidTransactionId, "COMMIT");

				logicalrep_read_commit(&s, &commit);
				st->cur_xid = InvalidTransactionId;
				txn_finish(st, txn, LR_TXN_COMMITTED, commit.commit_lsn,
						   commit.end_lsn);
				break;
			}

		case LOGICAL_REP_MSG_PREPARE:
			{
				LogicalRepPreparedTxnData prepare;
				LrTxn	   *txn;
				int64		gxid;

				logicalrep_read_prepare(&s, &prepare);
				txn = txn_lookup(st, prepare.xid, false);
				if (txn == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("PREPARE for unknown transaction %u", prepare.xid)));
				if (txn->gxid == 0 && parse_gid(prepare.gid, &gxid))
					txn_set_gxid(st, txn, gxid, LR_TXN_TWOPHASE);
				st->cur_xid = InvalidTransactionId;
				txn_finish(st, txn, LR_TXN_PREPARED, prepare.prepare_lsn,
						   prepare.end_lsn);
				break;
			}

		case LOGICAL_REP_MSG_COMMIT_PREPARED:
			{
				LogicalRepCommitPreparedTxnData commit;
				LrTxn	   *txn;

				logicalrep_read_commit_prepared(&s, &commit);
				/* may be long gone: released after apply, or never buffered */
				txn = txn_lookup(st, commit.xid, false);
				if (txn)
				{
					if (txn->kind == LR_TXN_LOCAL && txn->state == LR_TXN_PREPARED)
					{
						/* a user's prepared transaction: ready now */
						txn_finish(st, txn, LR_TXN_COMMITTED, commit.commit_lsn,
								   commit.end_lsn);
					}
					else
						txn->state = LR_TXN_COMMITTED;
				}
				break;
			}

		case LOGICAL_REP_MSG_ROLLBACK_PREPARED:
			{
				LogicalRepRollbackPreparedTxnData rollback;
				LrTxn	   *txn;

				logicalrep_read_rollback_prepared(&s, &rollback);
				txn = txn_lookup(st, rollback.xid, false);
				if (txn)
					txn_drop(st, txn);	/* no commit event will ever come */
				break;
			}

		case LOGICAL_REP_MSG_RELATION:
			handle_relation(st, &s);
			if (st->in_stream || TransactionIdIsValid(st->cur_xid))
			{
				/*
				 * A relation re-described inside a transaction that already
				 * has changes for it: keep the new description in order.
				 */
				LrTxn	   *txn = current_txn(st, subxid, "RELATION");
				Oid			relid;
				StringInfoData peek = s;

				peek.cursor = 1 + (st->in_stream ? 4 : 0);
				relid = pq_getmsgint(&peek, 4);
				if (list_member_oid(txn->rels_described, relid))
					txn_append(st, txn, LR_OP_RELATION, relid,
							   s.data + peek.cursor - 4, s.len - (peek.cursor - 4));
			}
			break;

		case LOGICAL_REP_MSG_TYPE:
		case LOGICAL_REP_MSG_ORIGIN:
			/* the writer ignores both (apply_handle_type / _origin) */
			break;

		case LOGICAL_REP_MSG_INSERT:
		case LOGICAL_REP_MSG_UPDATE:
		case LOGICAL_REP_MSG_DELETE:
			{
				LrTxn	   *txn = current_txn(st, subxid, logicalrep_message_type(type));
				const char *body = s.data + s.cursor;
				int			bodylen = s.len - s.cursor;
				Oid			relid = pq_getmsgint(&s, 4);	/* the body starts with it */

				txn_note_subxact(st, txn, subxid);
				txn_describe_rel(st, txn, relid);
				txn_append(st, txn, type, relid, body, bodylen);
				txn->nchanges++;
				break;
			}

		case LOGICAL_REP_MSG_TRUNCATE:
			{
				LrTxn	   *txn = current_txn(st, subxid, "TRUNCATE");
				const char *body = s.data + s.cursor;
				int			bodylen = s.len - s.cursor;
				bool		cascade,
							restart_seqs;
				List	   *relids;
				ListCell   *lc;

				relids = logicalrep_read_truncate(&s, &cascade, &restart_seqs);
				txn_note_subxact(st, txn, subxid);
				foreach(lc, relids)
					txn_describe_rel(st, txn, lfirst_oid(lc));
				txn->has_truncate = true;
				txn_append(st, txn, LR_OP_TRUNCATE, InvalidOid, body, bodylen);
				txn->nchanges++;
				break;
			}

		case LOGICAL_REP_MSG_MESSAGE:
			{
				uint8		flags = pq_getmsgint(&s, 1);
				const char *prefix;
				int			sz;
				const char *content;

				(void) pq_getmsgint64(&s);	/* lsn */
				prefix = pq_getmsgstring(&s);
				sz = pq_getmsgint(&s, 4);
				content = pq_getmsgbytes(&s, sz);

				if ((flags & LR_MESSAGE_TRANSACTIONAL) &&
					strcmp(prefix, LR_GXID_MESSAGE_PREFIX) == 0)
				{
					LrTxn	   *txn = current_txn(st, subxid, "cbdb.gxid");
					char	   *text = pnstrdup(content, sz);
					int64		gxid;

					if (!parse_gid(text, &gxid))
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("invalid cbdb.gxid message \"%s\"", text)));
					txn_set_gxid(st, txn, gxid,
								 txn->kind == LR_TXN_TWOPHASE ?
								 LR_TXN_TWOPHASE : LR_TXN_ONEPHASE);
				}
				/* other logical messages are not replicated */
				break;
			}

		case LOGICAL_REP_MSG_STREAM_START:
			{
				bool		first_segment;
				TransactionId xid = logicalrep_read_stream_start(&s, &first_segment);
				LrTxn	   *txn = txn_lookup(st, xid, true);

				if (XLogRecPtrIsInvalid(txn->first_lsn))
					txn->first_lsn = walend;
				txn->streamed = true;
				st->in_stream = true;
				st->stream_xid = xid;
				break;
			}

		case LOGICAL_REP_MSG_STREAM_STOP:
			st->in_stream = false;
			st->stream_xid = InvalidTransactionId;
			break;

		case LOGICAL_REP_MSG_STREAM_COMMIT:
			{
				LogicalRepCommitData commit;
				TransactionId xid = logicalrep_read_stream_commit(&s, &commit);
				LrTxn	   *txn = txn_lookup(st, xid, false);

				if (txn == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("STREAM COMMIT for unknown transaction %u", xid)));
				txn_finish(st, txn, LR_TXN_COMMITTED, commit.commit_lsn,
						   commit.end_lsn);
				break;
			}

		case LOGICAL_REP_MSG_STREAM_PREPARE:
			{
				LogicalRepPreparedTxnData prepare;
				LrTxn	   *txn;
				int64		gxid;

				logicalrep_read_stream_prepare(&s, &prepare);
				txn = txn_lookup(st, prepare.xid, false);
				if (txn == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("STREAM PREPARE for unknown transaction %u", prepare.xid)));
				if (txn->gxid == 0 && parse_gid(prepare.gid, &gxid))
					txn_set_gxid(st, txn, gxid, LR_TXN_TWOPHASE);
				txn_finish(st, txn, LR_TXN_PREPARED, prepare.prepare_lsn,
						   prepare.end_lsn);
				break;
			}

		case LOGICAL_REP_MSG_STREAM_ABORT:
			{
				LogicalRepStreamAbortData abort;
				LrTxn	   *txn;

				/* we request streaming "on", not "parallel": no abort info */
				logicalrep_read_stream_abort(&s, &abort, false);
				txn = txn_lookup(st, abort.xid, false);
				if (txn)
				{
					if (abort.subxid == abort.xid)
						txn_drop(st, txn);
					else
						txn_abort_subxact(st, txn, abort.subxid);
				}
				break;
			}

		default:
			ereport(ERROR,
					(errcode(ERRCODE_PROTOCOL_VIOLATION),
					 errmsg("invalid logical replication message type \"??? (%d)\"",
							(int) type)));
	}

	MemoryContextSwitchTo(oldcxt);
}

/*
 * The transaction a change-level message belongs to: the streamed one whose
 * block is open, else the non-streamed one in progress.
 */
static LrTxn *
current_txn(LrReaderState *st, TransactionId subxid, const char *what)
{
	LrTxn	   *txn = NULL;

	if (st->in_stream)
		txn = txn_lookup(st, st->stream_xid, false);
	else if (TransactionIdIsValid(st->cur_xid))
		txn = txn_lookup(st, st->cur_xid, false);

	if (txn == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("%s message outside of a transaction", what)));
	return txn;
}

static void
handle_relation(LrReaderState *st, StringInfo s)
{
	const char *body = s->data + s->cursor;
	int			bodylen = s->len - s->cursor;
	LogicalRepRelation *rel = logicalrep_read_rel(s);
	LrRel	   *lrel;
	bool		found;
	MemoryContext oldcxt;

	lrel = (LrRel *) hash_search(st->rels, &rel->remoteid, HASH_ENTER, &found);
	if (found)
	{
		lrel->generation++;
		pfree(lrel->raw);
		pfree(lrel->rel->nspname);
		pfree(lrel->rel->relname);
		for (int i = 0; i < lrel->rel->natts; i++)
			pfree(lrel->rel->attnames[i]);
		pfree(lrel->rel->attnames);
		pfree(lrel->rel->atttyps);
		bms_free(lrel->rel->attkeys);
		pfree(lrel->rel);
	}

	oldcxt = MemoryContextSwitchTo(st->cxt);
	lrel->relid = rel->remoteid;
	if (!found)
		lrel->generation = 1;
	lrel->raw = palloc(bodylen);
	memcpy(lrel->raw, body, bodylen);
	lrel->rawlen = bodylen;
	lrel->rel = palloc(sizeof(LogicalRepRelation));
	*lrel->rel = *rel;
	lrel->rel->nspname = pstrdup(rel->nspname);
	lrel->rel->relname = pstrdup(rel->relname);
	lrel->rel->attnames = palloc(rel->natts * sizeof(char *));
	lrel->rel->atttyps = palloc(rel->natts * sizeof(Oid));
	for (int i = 0; i < rel->natts; i++)
	{
		lrel->rel->attnames[i] = pstrdup(rel->attnames[i]);
		lrel->rel->atttyps[i] = rel->atttyps[i];
	}
	lrel->rel->attkeys = bms_copy(rel->attkeys);
	MemoryContextSwitchTo(oldcxt);
}

/* the gid of a Cloudberry two-phase segment transaction is the gxid in decimal */
static bool
parse_gid(const char *gid, int64 *gxid)
{
	char	   *end;
	unsigned long long v;

	if (gid == NULL || *gid == '\0')
		return false;
	errno = 0;
	v = strtoull(gid, &end, 10);
	if (errno != 0 || *end != '\0' || v == 0)
		return false;
	*gxid = (int64) v;
	return true;
}

/* ------------------------------------------------------------------------
 * Transactions
 * ------------------------------------------------------------------------
 */

static LrTxn *
txn_lookup(LrReaderState *st, TransactionId xid, bool create)
{
	LrTxn	   *txn;
	bool		found;

	if (!create)
		return (LrTxn *) hash_search(st->xids, &xid, HASH_FIND, NULL);

	txn = (LrTxn *) hash_search(st->xids, &xid, HASH_ENTER, &found);
	if (!found)
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(st->cxt);

		memset(txn, 0, sizeof(LrTxn));
		txn->xid = xid;
		txn->state = LR_TXN_OPEN;
		txn->kind = LR_TXN_KIND_UNKNOWN;
		txn->changes = palloc0(sizeof(LrChangeLog));
		changelog_init(txn->changes);
		MemoryContextSwitchTo(oldcxt);
	}
	return txn;
}

static void
txn_set_gxid(LrReaderState *st, LrTxn *txn, int64 gxid, LrTxnKind kind)
{
	LrGxidEntry *ge;
	bool		found;

	if (txn->gxid != 0 && txn->gxid != gxid)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("transaction %u carries two global ids " INT64_FORMAT " and " INT64_FORMAT,
						txn->xid, txn->gxid, gxid)));

	txn->gxid = gxid;
	txn->kind = kind;

	ge = (LrGxidEntry *) hash_search(st->gxids, &gxid, HASH_ENTER, &found);
	if (found && ge->txn != txn)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("global id " INT64_FORMAT " seen for transactions %u and %u",
						gxid, ge->txn->xid, txn->xid)));
	ge->txn = txn;
}

static bool
txn_is_ready(const LrTxn *txn)
{
	switch (txn->kind)
	{
		case LR_TXN_TWOPHASE:
		case LR_TXN_ONEPHASE:
			return txn->gxid != 0 && txn->state != LR_TXN_OPEN;
		case LR_TXN_LOCAL:
			return txn->state == LR_TXN_COMMITTED;
		case LR_TXN_KIND_UNKNOWN:
			break;
	}
	return false;
}

/*
 * COMMIT / PREPARE arrived.  Classify, deduplicate against the replay start
 * point, and queue the transaction for reporting.
 */
static void
txn_finish(LrReaderState *st, LrTxn *txn, LrTxnState state,
		   XLogRecPtr commit_lsn, XLogRecPtr end_lsn)
{
	MemoryContext oldcxt;

	txn->state = state;
	txn->commit_lsn = commit_lsn;
	txn->end_lsn = end_lsn;
	if (txn->kind == LR_TXN_KIND_UNKNOWN)
		txn->kind = LR_TXN_LOCAL;

	/*
	 * Replay dedup, and for a local transaction the initial copy's as well.
	 * A transaction with a gxid is judged by the driver against the
	 * distributed snapshot of the cut point; one without has no gxid to judge
	 * and is judged here instead, against this segment's own snapshot at the
	 * same cut point.  Both answer the one question: is it in the copy
	 * already?
	 */
	if (txn->gxid != 0)
	{
		if (hash_search(st->applied, &txn->gxid, HASH_FIND, NULL) != NULL)
		{
			txn_drop(st, txn);
			return;
		}
	}
	else if (txn_covered_by_sync(st, txn->xid) ||
			 (!XLogRecPtrIsInvalid(st->replay_seg_pos) &&
			  commit_lsn <= st->replay_seg_pos))
	{
		txn_drop(st, txn);
		return;
	}

	if (!txn_is_ready(txn) || txn->reported)
		return;

	oldcxt = MemoryContextSwitchTo(st->cxt);
	st->pending = lappend(st->pending, txn);
	if (txn->kind == LR_TXN_LOCAL)
		st->local_ready = lappend(st->local_ready, txn);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Was this local transaction already in the initial copy?
 *
 * The copy ran under this segment's snapshot, frozen at the cut point, so
 * "the copy has it" is exactly "it is visible under that snapshot" -- the
 * same shape as the question the driver asks of a gxid against the
 * distributed snapshot.  Self-limiting: everything that committed after the
 * cut point is at or above xmax and falls out on the first test, so the scan
 * below stops mattering shortly after a synchronisation ends.
 */
static bool
txn_covered_by_sync(LrReaderState *st, TransactionId xid)
{
	if (!TransactionIdIsValid(st->sync_xmax) || !TransactionIdIsValid(xid))
		return false;
	if (!TransactionIdPrecedes(xid, st->sync_xmax))
		return false;			/* committed after the cut point */
	if (TransactionIdPrecedes(xid, st->sync_xmin))
		return true;			/* long finished by then */
	for (int i = 0; i < st->nsync_xip; i++)
		if (TransactionIdEquals(xid, st->sync_xip[i]))
			return false;		/* in flight at the cut point: not copied */
	return true;
}

static void
txn_drop(LrReaderState *st, LrTxn *txn)
{
	if (txn->gxid != 0)
	{
		LrGxidEntry *ge = (LrGxidEntry *) hash_search(st->gxids, &txn->gxid,
													  HASH_FIND, NULL);

		if (ge && ge->txn == txn)
			hash_search(st->gxids, &txn->gxid, HASH_REMOVE, NULL);
	}
	st->pending = list_delete_ptr(st->pending, txn);
	st->local_ready = list_delete_ptr(st->local_ready, txn);
	if (st->cur_xid == txn->xid)
		st->cur_xid = InvalidTransactionId;
	if (st->stream_xid == txn->xid)
	{
		st->stream_xid = InvalidTransactionId;
		st->in_stream = false;
	}

	changelog_free(st, txn->changes);
	pfree(txn->changes);
	list_free(txn->relids);
	list_free(txn->rels_described);
	list_free_deep(txn->subxacts);
	hash_search(st->xids, &txn->xid, HASH_REMOVE, NULL);
}

/*
 * Make sure the transaction carries the description of relid before its
 * first change of that relation.
 */
static void
txn_describe_rel(LrReaderState *st, LrTxn *txn, Oid relid)
{
	LrRel	   *lrel;
	MemoryContext oldcxt;

	if (list_member_oid(txn->rels_described, relid))
		return;

	lrel = (LrRel *) hash_search(st->rels, &relid, HASH_FIND, NULL);
	if (lrel == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("change for relation %u arrived before its RELATION message",
						relid)));

	txn_append(st, txn, LR_OP_RELATION, relid, lrel->raw, lrel->rawlen);

	oldcxt = MemoryContextSwitchTo(st->cxt);
	txn->rels_described = lappend_oid(txn->rels_described, relid);
	if (!list_member_oid(txn->relids, relid))
		txn->relids = lappend_oid(txn->relids, relid);
	MemoryContextSwitchTo(oldcxt);
}

static void
txn_append(LrReaderState *st, LrTxn *txn, char op, Oid relid,
		   const char *body, int len)
{
	LrRecordHeader hdr;

	hdr.op = (uint8) op;
	hdr.relid = relid;
	hdr.len = (uint32) len;
	changelog_append(st, txn->changes, &hdr, sizeof(hdr));
	changelog_append(st, txn->changes, body, len);
	txn->nbytes += sizeof(hdr) + len;
	maybe_spill(st);
}

/* streaming: remember where a sub-transaction's changes start */
static void
txn_note_subxact(LrReaderState *st, LrTxn *txn, TransactionId subxid)
{
	ListCell   *lc;
	LrSubxact  *sx;
	MemoryContext oldcxt;

	if (!TransactionIdIsValid(subxid) || subxid == txn->xid)
		return;
	foreach(lc, txn->subxacts)
	{
		if (((LrSubxact *) lfirst(lc))->subxid == subxid)
			return;
	}
	oldcxt = MemoryContextSwitchTo(st->cxt);
	sx = palloc(sizeof(LrSubxact));
	sx->subxid = subxid;
	sx->offset = txn->changes->total_len;
	sx->nchanges = txn->nchanges;
	sx->nbytes = txn->nbytes;
	txn->subxacts = lappend(txn->subxacts, sx);
	MemoryContextSwitchTo(oldcxt);
}

/*
 * Discard the changes of an aborted sub-transaction: everything from its
 * first change on, as worker.c does (later changes belong to it or to
 * sub-transactions nested in it).  relids may stay a superset.
 */
static void
txn_abort_subxact(LrReaderState *st, LrTxn *txn, TransactionId subxid)
{
	ListCell   *lc;
	LrSubxact  *start = NULL;
	List	   *keep = NIL;
	MemoryContext oldcxt;

	foreach(lc, txn->subxacts)
	{
		LrSubxact  *sx = (LrSubxact *) lfirst(lc);

		if (sx->subxid == subxid)
			start = sx;
	}
	if (start == NULL)
		return;					/* the sub-transaction never wrote anything */

	changelog_truncate(st, txn->changes, start->offset);
	txn->nchanges = start->nchanges;
	txn->nbytes = start->nbytes;

	/* R records past the cut are gone too */
	oldcxt = MemoryContextSwitchTo(st->cxt);
	foreach(lc, txn->subxacts)
	{
		LrSubxact  *sx = (LrSubxact *) lfirst(lc);

		if (sx->offset < start->offset)
			keep = lappend(keep, sx);
		else
			pfree(sx);
	}
	list_free(txn->subxacts);
	txn->subxacts = keep;
	list_free(txn->rels_described);
	txn->rels_described = NIL;
	MemoryContextSwitchTo(oldcxt);

	/* rebuild rels_described from what is left */
	{
		LrChangeCursor cur;
		char		op;
		Oid			relid;
		const char *data;
		int			len;

		LrChangesBegin(txn, &cur);
		while (LrChangesNext(&cur, &op, &relid, &data, &len))
		{
			if (op == LR_OP_RELATION)
			{
				oldcxt = MemoryContextSwitchTo(st->cxt);
				txn->rels_described = lappend_oid(txn->rels_described, relid);
				MemoryContextSwitchTo(oldcxt);
			}
		}
		LrChangesEnd(&cur);
	}
}

LrGxidStatus
LrReaderGxidStatus(LrReaderState *st, int64 gxid)
{
	LrGxidEntry *ge;

	if (hash_search(st->applied, &gxid, HASH_FIND, NULL) != NULL)
		return LR_GXID_APPLIED;
	ge = (LrGxidEntry *) hash_search(st->gxids, &gxid, HASH_FIND, NULL);
	if (ge == NULL)
		return LR_GXID_UNKNOWN;
	return txn_is_ready(ge->txn) ? LR_GXID_READY : LR_GXID_OPEN;
}

/*
 * Transactions that became ready since the last call, in the order they
 * completed.  The list is palloc'd in the caller's context.
 */
List *
LrReaderCollectReady(LrReaderState *st)
{
	List	   *result = NIL;
	ListCell   *lc;

	foreach(lc, st->pending)
	{
		LrTxn	   *txn = (LrTxn *) lfirst(lc);

		txn->reported = true;
		result = lappend(result, txn);
	}
	list_free(st->pending);
	st->pending = NIL;
	return result;
}

LrTxn *
LrReaderFindGxid(LrReaderState *st, int64 gxid)
{
	LrGxidEntry *ge = (LrGxidEntry *) hash_search(st->gxids, &gxid, HASH_FIND,
												  NULL);

	return ge ? ge->txn : NULL;
}

LrTxn *
LrReaderFindLocal(LrReaderState *st, XLogRecPtr commit_lsn)
{
	ListCell   *lc;

	foreach(lc, st->local_ready)
	{
		LrTxn	   *txn = (LrTxn *) lfirst(lc);

		if (txn->commit_lsn == commit_lsn)
			return txn;
	}
	return NULL;
}

/* the coordinator applied and committed it: forget it */
void
LrReaderRelease(LrReaderState *st, LrTxn *txn)
{
	txn_drop(st, txn);
}

void
LrReaderFillReport(LrReaderState *st, LrTxn *txn, int seg, LrReport *rpt)
{
	ListCell   *lc;

	memset(rpt, 0, sizeof(LrReport));
	rpt->seg = seg;
	rpt->kind = txn->kind == LR_TXN_LOCAL ? LR_REPORT_LOCAL : LR_REPORT_READY;

	/*
	 * A utility write has no gxid.  What identifies it against a cut point is
	 * its own xid on this segment, so that is what the field carries for one;
	 * see LrReport.
	 */
	rpt->gxid = txn->kind == LR_TXN_LOCAL ? (int64) txn->xid : txn->gxid;
	rpt->seg_lsn = txn->commit_lsn;
	rpt->remote_relids = list_copy(txn->relids);
	foreach(lc, txn->relids)
	{
		LrRel	   *lrel = (LrRel *) hash_search(st->rels, &lfirst_oid(lc),
												 HASH_FIND, NULL);

		rpt->rel_names = lappend(rpt->rel_names,
								 lrel ? psprintf("%s.%s", lrel->rel->nspname,
												 lrel->rel->relname)
								 : pstrdup("?"));
	}
	rpt->lockmode = txn->has_truncate ? AccessExclusiveLock : RowExclusiveLock;
	rpt->nrows = txn->nchanges;
	rpt->nbytes = txn->nbytes;
}

/* CONNECT: which source segment this reader serves (-1 = none) */

int
LrReaderSourceSeg(LrReaderState *st)
{
	return st->source_seg;
}

bool
LrReaderHasSource(LrReaderState *st)
{
	return st->source_seg >= 0;
}

/* CONNECT: the source relations that are DISTRIBUTED REPLICATED */
void
LrReaderSetReplicatedRels(LrReaderState *st, List *relids)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(st->cxt);

	list_free(st->replicated_rels);
	st->replicated_rels = list_copy(relids);
	MemoryContextSwitchTo(oldcxt);
}

bool
LrReaderRelIsReplicated(LrReaderState *st, Oid relid)
{
	return list_member_oid(st->replicated_rels, relid);
}

List *
LrReaderAllTxns(LrReaderState *st)
{
	HASH_SEQ_STATUS seq;
	LrTxn	   *txn;
	List	   *result = NIL;

	hash_seq_init(&seq, st->xids);
	while ((txn = (LrTxn *) hash_seq_search(&seq)) != NULL)
		result = lappend(result, txn);
	return result;
}

const LogicalRepRelation *
LrReaderRemoteRel(LrReaderState *st, Oid relid)
{
	LrRel	   *lrel = (LrRel *) hash_search(st->rels, &relid, HASH_FIND, NULL);

	return lrel ? lrel->rel : NULL;
}

uint32
LrReaderRelGeneration(LrReaderState *st, Oid relid)
{
	LrRel	   *lrel = (LrRel *) hash_search(st->rels, &relid, HASH_FIND, NULL);

	return lrel ? lrel->generation : 0;
}

bool
LrReaderRelRaw(LrReaderState *st, Oid relid, const char **data, int *len)
{
	LrRel	   *lrel = (LrRel *) hash_search(st->rels, &relid, HASH_FIND, NULL);

	if (lrel == NULL)
		return false;
	*data = lrel->raw;
	*len = lrel->rawlen;
	return true;
}

void
LrReaderGetStats(LrReaderState *st, LrReaderStats *stats)
{
	HASH_SEQ_STATUS seq;
	LrTxn	   *txn;

	memset(stats, 0, sizeof(*stats));
	hash_seq_init(&seq, st->xids);
	while ((txn = (LrTxn *) hash_seq_search(&seq)) != NULL)
	{
		stats->ntxns++;
		if (txn->state == LR_TXN_OPEN)
			stats->nopen++;
		else if (txn->kind == LR_TXN_LOCAL)
			stats->nready_local++;
		else
			stats->nready_gxid++;
		if (txn->changes->spilled)
			stats->nspilled++;
	}
	stats->nrels = hash_get_num_entries(st->rels);
	stats->mem_bytes = st->mem_bytes;
	stats->spill_bytes = st->spill_bytes;
	stats->nmsgs = st->nmsgs;
	stats->pos = st->pos;
	stats->flush = LrReaderFlushPosition(st);
	stats->connected = st->conn != NULL;
	stats->eof = st->eof;
}

/* ------------------------------------------------------------------------
 * Change log
 * ------------------------------------------------------------------------
 */

static void
changelog_init(LrChangeLog *log)
{
	log->mem = NULL;
	log->mem_alloc = 0;
	log->file = NULL;
	log->spilled = false;
	log->total_len = 0;
}

static void
changelog_append(LrReaderState *st, LrChangeLog *log, const void *data, Size len)
{
	if (log->spilled)
	{
		/* a truncation or a cursor may have moved the position */
		if (BufFileSeek(log->file, 0, (off_t) log->total_len, SEEK_SET) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not seek in reader spill file")));
		BufFileWrite(log->file, data, len);
		st->spill_bytes += len;
	}
	else
	{
		if (log->total_len + len > log->mem_alloc)
		{
			Size		want = Max(log->mem_alloc * 2, (Size) 8192);

			while (want < log->total_len + len)
				want *= 2;
			if (log->mem == NULL)
				log->mem = MemoryContextAlloc(st->cxt, want);
			else
				log->mem = repalloc(log->mem, want);
			log->mem_alloc = want;
		}
		memcpy(log->mem + log->total_len, data, len);
		st->mem_bytes += len;
	}
	log->total_len += len;
}

static void
changelog_spill(LrReaderState *st, LrChangeLog *log)
{
	MemoryContext oldcxt;

	if (log->spilled)
		return;

	/*
	 * Inter-transaction: the file must outlive the statement that made it.
	 * The BufFile itself must live in the state's context, not in the
	 * per-message scratch context this is called from.
	 */
	oldcxt = MemoryContextSwitchTo(st->cxt);
	log->file = BufFileCreateTemp("mpp_apply_reader", true);
	MemoryContextSwitchTo(oldcxt);
	if (log->total_len > 0)
		BufFileWrite(log->file, log->mem, log->total_len);
	st->mem_bytes -= log->total_len;
	st->spill_bytes += log->total_len;
	st->nspilled++;

	if (log->mem)
		pfree(log->mem);
	log->mem = NULL;
	log->mem_alloc = 0;
	log->spilled = true;
}

static void
changelog_truncate(LrReaderState *st, LrChangeLog *log, Size len)
{
	if (len >= log->total_len)
		return;
	if (log->spilled)
		st->spill_bytes -= log->total_len - len;
	else
		st->mem_bytes -= log->total_len - len;
	log->total_len = len;
}

static void
changelog_free(LrReaderState *st, LrChangeLog *log)
{
	if (log->spilled)
	{
		st->spill_bytes -= log->total_len;
		st->nspilled--;
		BufFileClose(log->file);
		log->file = NULL;
	}
	else
	{
		st->mem_bytes -= log->total_len;
		if (log->mem)
			pfree(log->mem);
		log->mem = NULL;
	}
	log->total_len = 0;
	log->spilled = false;
}

/* over budget: spill the largest in-memory transactions until under it */
static void
maybe_spill(LrReaderState *st)
{
	Size		budget = (Size) gp_lr_reader_mem * 1024;

	while (st->mem_bytes > budget)
	{
		HASH_SEQ_STATUS seq;
		LrTxn	   *txn;
		LrTxn	   *largest = NULL;

		hash_seq_init(&seq, st->xids);
		while ((txn = (LrTxn *) hash_seq_search(&seq)) != NULL)
		{
			if (txn->changes->spilled)
				continue;
			if (largest == NULL ||
				txn->changes->total_len > largest->changes->total_len)
				largest = txn;
		}
		if (largest == NULL || largest->changes->total_len == 0)
			break;
		changelog_spill(st, largest->changes);
	}
}

void
LrChangesBegin(LrTxn *txn, LrChangeCursor *cur)
{
	cur->txn = txn;
	cur->offset = 0;
	cur->buf = NULL;
	cur->bufsize = 0;
	if (txn->changes->spilled &&
		BufFileSeek(txn->changes->file, 0, 0, SEEK_SET) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not rewind reader spill file")));
}

bool
LrChangesNext(LrChangeCursor *cur, char *op, Oid *relid, const char **data,
			  int *len)
{
	LrChangeLog *log = cur->txn->changes;
	LrRecordHeader hdr;

	if (cur->offset + sizeof(hdr) > log->total_len)
		return false;

	if (log->spilled)
	{
		BufFileReadExact(log->file, &hdr, sizeof(hdr));
		if (cur->offset + sizeof(hdr) + hdr.len > log->total_len)
			elog(ERROR, "corrupt reader spill file");
		if (hdr.len > cur->bufsize)
		{
			cur->buf = cur->buf ? repalloc(cur->buf, hdr.len) : palloc(hdr.len);
			cur->bufsize = hdr.len;
		}
		if (hdr.len > 0)
			BufFileReadExact(log->file, cur->buf, hdr.len);
		*data = cur->buf;
	}
	else
	{
		memcpy(&hdr, log->mem + cur->offset, sizeof(hdr));
		if (cur->offset + sizeof(hdr) + hdr.len > log->total_len)
			elog(ERROR, "corrupt reader change log");
		*data = log->mem + cur->offset + sizeof(hdr);
	}

	*op = (char) hdr.op;
	*relid = hdr.relid;
	*len = (int) hdr.len;
	cur->offset += sizeof(hdr) + hdr.len;
	return true;
}

void
LrChangesEnd(LrChangeCursor *cur)
{
	if (cur->buf)
		pfree(cur->buf);
	cur->buf = NULL;
	cur->bufsize = 0;
}
