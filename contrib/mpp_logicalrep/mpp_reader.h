/*-------------------------------------------------------------------------
 *
 * mpp_reader.h
 *	  Reader state of MPP logical replication apply: one source segment's
 *	  pgoutput stream, buffered by transaction until the coordinator asks
 *	  for it.
 *
 * A reader QE (design §7.3) keeps one LrReaderState per (subscription,
 * source segment) in process-private memory -- more than one when the
 * source has more segments than the target, and one with no stream at all
 * when it has fewer (design §9.2b).  The state owns the walreceiver
 * connection to the source segment and buffers every transaction by xid
 * until the LogicalSource operator (mpp_source.c) reports it ready
 * (DRAIN), emits its changes (APPLY) and finally releases it.
 *
 * Transactions are classified by how their global id becomes known:
 *   two-phase   the gid of BEGIN PREPARE / PREPARE is the gxid in decimal
 *   one-phase   a transactional logical message "cbdb.gxid" carries it
 *   local       COMMIT without any gxid (utility-mode writes, or a sender
 *               without the cbdb.gxid patch)
 * A two-phase transaction is ready at PREPARE, a one-phase or local one at
 * COMMIT.  Replay after a restart is deduplicated with the start point the
 * coordinator hands over (design §13.1).
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * contrib/mpp_logicalrep/mpp_reader.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MPP_READER_H
#define MPP_READER_H

#include "access/xlogdefs.h"
#include "nodes/pg_list.h"
#include "replication/logicalproto.h"
#include "mpp_apply.h"

/* GUC: per-subscription budget for buffered changes before spilling, in kB */
extern int	gp_lr_reader_mem;

typedef enum LrTxnState
{
	LR_TXN_OPEN,				/* changes still arriving */
	LR_TXN_PREPARED,			/* PREPARE seen (two-phase): ready */
	LR_TXN_COMMITTED,			/* COMMIT / COMMIT PREPARED seen: ready */
} LrTxnState;

typedef enum LrTxnKind
{
	LR_TXN_KIND_UNKNOWN,		/* still open, no gxid yet */
	LR_TXN_TWOPHASE,			/* gxid from the gid */
	LR_TXN_ONEPHASE,			/* gxid from cbdb.gxid */
	LR_TXN_LOCAL,				/* no gxid */
} LrTxnKind;

struct LrChangeLog;

/*
 * One buffered transaction.  The changes are kept in the order received as
 * records (op, relid, body); the body is the pgoutput message body without
 * the type byte (and without the streaming xid), i.e. what the writer
 * expects on the channel (mpp_apply.h).  Every relation a transaction
 * touches is described by an R record inside the transaction before its
 * first change, so a transaction is self-contained for the writer.
 */
typedef struct LrTxn
{
	TransactionId xid;			/* hash key: top-level xid in the stream */
	int64		gxid;			/* 0 = unknown / local */
	LrTxnState	state;
	LrTxnKind	kind;
	XLogRecPtr	first_lsn;		/* WAL end of the first message seen */
	XLogRecPtr	commit_lsn;		/* position of the COMMIT / PREPARE record;
								 * identity of a local transaction and the
								 * replay boundary (§13.1) */
	XLogRecPtr	end_lsn;		/* end of that record */
	int64		nchanges;		/* I/U/D/T records */
	int64		nbytes;			/* bytes of all records */
	List	   *relids;			/* remote relids touched (Oid list) */
	bool		has_truncate;
	bool		streamed;		/* arrived through the streaming protocol */
	bool		reported;		/* handed to the coordinator as ready */
	struct LrChangeLog *changes;

	/* internal */
	List	   *rels_described; /* relids whose R record is in changes */
	List	   *subxacts;		/* LrSubxact*, streaming only */
} LrTxn;

typedef struct LrReaderState LrReaderState;

/*
 * Where to (re)start the stream and what to drop on replay (§13.1).
 */
typedef struct LrReaderStartPoint
{
	XLogRecPtr	seg_flush;		/* start LSN; Invalid = the slot's confirmed_flush */
	XLogRecPtr	seg_pos;		/* local transactions at or below are applied */
	int			napplied;
	const int64 *applied;		/* gxids applied above seg_flush */
} LrReaderStartPoint;

typedef enum LrReceiveResult
{
	LR_RECV_TARGET_READY,		/* the requested gxid is ready (or applied) */
	LR_RECV_DRAINED,			/* nothing more buffered on the socket */
	LR_RECV_TIMEOUT,			/* wait_ms elapsed */
	LR_RECV_EOF,				/* connection lost */
} LrReceiveResult;

typedef enum LrGxidStatus
{
	LR_GXID_UNKNOWN,			/* nothing seen for it */
	LR_GXID_OPEN,				/* seen, not yet ready */
	LR_GXID_READY,
	LR_GXID_APPLIED,			/* in the replay set: already applied */
} LrGxidStatus;

typedef struct LrReaderStats
{
	int			ntxns;
	int			nopen;
	int			nready_gxid;	/* ready with a gxid, not released */
	int			nready_local;
	int			nspilled;
	int			nrels;
	int64		mem_bytes;
	int64		spill_bytes;
	int64		nmsgs;
	XLogRecPtr	pos;			/* read position */
	XLogRecPtr	flush;			/* safe confirmed_flush */
	bool		connected;
	bool		eof;
} LrReaderStats;

/* iteration over a transaction's records */
typedef struct LrChangeCursor
{
	LrTxn	   *txn;
	Size		offset;
	char	   *buf;			/* spilled: record body buffer */
	Size		bufsize;
} LrChangeCursor;

/* lifetime */
extern LrReaderState *LrReaderCreate(Oid subid, int srcseg, int64 token,
									 const LrReaderStartPoint *start);
extern LrReaderState *LrReaderFind(Oid subid, int srcseg);
extern List *LrReaderListSub(Oid subid);
extern void LrReaderDestroy(LrReaderState *st);
extern void LrReaderDestroySub(Oid subid);
extern Oid	LrReaderSubid(LrReaderState *st);
extern int64 LrReaderToken(LrReaderState *st);

/* stream */
extern void LrReaderConnect(LrReaderState *st, const char *conninfo,
							const char *slotname, List *publications,
							bool binary, bool streaming, bool twophase);
extern bool LrReaderIsConnected(LrReaderState *st);
extern LrReceiveResult LrReaderReceive(LrReaderState *st, int64 target_gxid,
									   int wait_ms, bool stop_on_any_ready);
extern void LrReaderProcessMessage(LrReaderState *st, const char *data, int len,
								   XLogRecPtr walend);
extern void LrReaderSendFeedback(LrReaderState *st);
extern XLogRecPtr LrReaderFlushPosition(LrReaderState *st);
extern XLogRecPtr LrReaderReadPosition(LrReaderState *st);

/* transactions */
extern LrGxidStatus LrReaderGxidStatus(LrReaderState *st, int64 gxid);
extern List *LrReaderCollectReady(LrReaderState *st);
extern LrTxn *LrReaderFindGxid(LrReaderState *st, int64 gxid);
extern LrTxn *LrReaderFindLocal(LrReaderState *st, XLogRecPtr commit_lsn);
extern void LrReaderRelease(LrReaderState *st, LrTxn *txn);
extern void LrReaderFillReport(LrReaderState *st, LrTxn *txn, int seg,
							   LrReport *rpt);
extern List *LrReaderAllTxns(LrReaderState *st);
extern int	LrReaderSourceSeg(LrReaderState *st);
extern bool LrReaderHasSource(LrReaderState *st);
extern void LrReaderSetReplicatedRels(LrReaderState *st, List *relids);
extern bool LrReaderRelIsReplicated(LrReaderState *st, Oid relid);

/* relations described by the stream */
extern const LogicalRepRelation *LrReaderRemoteRel(LrReaderState *st, Oid relid);
extern uint32 LrReaderRelGeneration(LrReaderState *st, Oid relid);
extern bool LrReaderRelRaw(LrReaderState *st, Oid relid, const char **data,
						   int *len);

/* records */
extern void LrChangesBegin(LrTxn *txn, LrChangeCursor *cur);
extern bool LrChangesNext(LrChangeCursor *cur, char *op, Oid *relid,
						  const char **data, int *len);
extern void LrChangesEnd(LrChangeCursor *cur);
extern bool LrTxnIsSpilled(LrTxn *txn);

extern void LrReaderGetStats(LrReaderState *st, LrReaderStats *stats);

#endif							/* MPP_READER_H */
