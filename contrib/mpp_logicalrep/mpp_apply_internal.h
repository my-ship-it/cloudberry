/*-------------------------------------------------------------------------
 *
 * mpp_apply_internal.h
 *	  What the Cloudberry assembly and the Cloudberry sink share.
 *
 * They are the two halves of one pairing and they
 * share exactly one thing: the target coordinator session.  They have to.
 * The reader gang that buffers the source streams and the writer gang that
 * applies them are slices of the same dispatched plan, so both gangs live
 * in one session; the assembly drives gp_lr_connect
 * and gp_lr_drain on it, the sink drives gp_lr_apply and gp_lr_progress.
 *
 * Nothing else crosses: the sink knows nothing of the registry, the source
 * streams or the deferred set, and the assembly knows nothing of locks,
 * transactions or the progress tables.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * contrib/mpp_logicalrep/mpp_apply_internal.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MPP_APPLY_INTERNAL_H
#define MPP_APPLY_INTERNAL_H

#include "libpq-fe.h"
#include "logicalrep_sink.h"
#include "mpp_apply_worker.h"

/*
 * The shared target session.  Owned by the sink (it opens it and it is the
 * one that can reopen it after an error); the assembly borrows it.
 */
typedef struct LrCbdbTransport
{
	PGconn	   *qd;				/* a normal coordinator session, over libpq */
	const char *conninfo;
	Oid			subid;
	int64		token;			/* identifies this driver to the gangs */
	int			nsegs;			/* target primary segments */
	LrWorkerStats *stats;
} LrCbdbTransport;

/* what the assembly tells the transport about one unit (transport_private) */
typedef struct LrCbdbUnitSpec
{
	/*
	 * Tables the reader gang may not emit for this unit (they are being
	 * copied), and tables it may emit only (a catch-up of exactly those).
	 * Both are oid[] literals of remote relids, or NULL.  Design 10.5.
	 */
	const char *skip_relids;
	const char *only_relids;
	bool		catchup;		/* this is a catch-up unit */
} LrCbdbUnitSpec;

/* the session (mpp_apply_worker.c) */
extern void LrTransportConnect(LrCbdbTransport *tr);
extern void LrTransportClose(LrCbdbTransport *tr);
extern PGresult *LrTransportExec(LrCbdbTransport *tr, const char *sql);
extern PGresult *LrTransportExecParams(LrCbdbTransport *tr, const char *sql,
									   int nparams, const char *const *params);
extern char *LrTransportQuoteRel(LrCbdbTransport *tr, const char *nspname,
								 const char *relname);

/* the two halves */
extern LogicalRepSink *LrMakeCbdbSink(LrCbdbTransport *tr);	/* mpp_sink_cbdb.c */
extern LogicalRepAssembly *LrMakeCbdbAssembly(LrCbdbTransport *tr,
											  LrWorkerConfig *cfg,
											  LrWorkerStats *stats);

#endif							/* MPP_APPLY_INTERNAL_H */
