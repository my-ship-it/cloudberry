/*-------------------------------------------------------------------------
 *
 * mpp_apply.h
 *	  MPP logical replication apply: plan shape, operators and reports.
 *
 * The receiving side of MPP logical replication drives the reader and
 * writer gangs of a normal QD session with one hand-built plan shape:
 *
 *   slice 0 (QD)           Gather Motion                  one report row per QE
 *   slice 1 (writer gang)  LogicalApply(mode, spec)
 *                            Explicit Redistribute Motion (segidColIdx = 1)
 *   slice 2 (reader gang)  LogicalSource(mode, spec)
 *
 * All three modes (CONNECT / DRAIN / APPLY) use exactly this shape so that
 * the reader slice keeps landing on the same idle QEs of the session.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * contrib/mpp_logicalrep/mpp_apply.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MPP_APPLY_H
#define MPP_APPLY_H

#include "access/htup.h"
#include "access/tupdesc.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "utils/tuplestore.h"

typedef enum LrPlanMode
{
	LR_PLAN_CONNECT = 0,		/* readers connect to their source segments */
	LR_PLAN_DRAIN,				/* readers read their stream, report readiness */
	LR_PLAN_APPLY				/* readers emit changes, writers apply them */
} LrPlanMode;

/*
 * What one plan carries down to the operators.  Serialized into the
 * CustomScan's custom_private, so it travels with the plan on dispatch.
 */
typedef struct LrPlanSpec
{
	LrPlanMode	mode;
	Oid			subid;			/* subscription */
	int64		token;			/* session token, checked by the readers */
	int			nsegs;			/* size of reader and writer gangs */
	const char *payload;		/* mode-specific text, owned by LogicalSource;
								 * may be NULL */
} LrPlanSpec;

/* Slice numbering fixed by the plan shape */
#define LR_SLICE_QD			0
#define LR_SLICE_WRITER		1
#define LR_SLICE_READER		2

/* Motion ids equal the sending slice index */
#define LR_MOTION_GATHER	LR_SLICE_WRITER
#define LR_MOTION_EXPLICIT	LR_SLICE_READER

/*
 * LogicalSource output: (target_segid int4, relid oid, op "char",
 * payload bytea, seq int8).  target_segid is the Explicit Motion's routing
 * column.
 */
#define LR_SRC_NATTS			5
#define LR_SRC_ATT_TARGET_SEGID	1
#define LR_SRC_ATT_RELID		2
#define LR_SRC_ATT_OP			3
#define LR_SRC_ATT_PAYLOAD		4
#define LR_SRC_ATT_SEQ			5

/* op codes of data rows */
#define LR_OP_INSERT		'I'
#define LR_OP_UPDATE		'U'
#define LR_OP_DELETE		'D'
#define LR_OP_TRUNCATE		'T'
#define LR_OP_RELATION		'R'
#define LR_OP_BEGIN			'B'
#define LR_OP_END			'E'

/*
 * op codes of report rows.  Reports produced by the readers travel through
 * the same channel as data rows (payload = serialized LrReport) and are
 * passed through by LogicalApply; LogicalApply adds its own.
 */
#define LR_REPORT_READY		'P' /* transaction with a gxid is ready */
#define LR_REPORT_LOCAL		'L' /* local (single-segment) transaction ready */
#define LR_REPORT_APPLIED	'A' /* writer applied a batch */
#define LR_REPORT_COLD		'X' /* reader has no state for this subscription */
#define LR_REPORT_CONNECTED	'C' /* reader connected (CONNECT) */
#define LR_REPORT_DEDUP		'Z' /* DRAIN: the target gxid is already applied
								 * here (replay); nothing to wait for */
#define LR_REPORT_STATUS	'S' /* reader position (DRAIN): seg_lsn = read
								 * position, gxid = flush position, nrows =
								 * buffered transactions, nbytes = buffered
								 * bytes (memory + spill) */

#define LR_OP_TYPE			'Y'

#define LrOpIsReport(op) \
	((op) == LR_REPORT_READY || (op) == LR_REPORT_LOCAL || \
	 (op) == LR_REPORT_APPLIED || (op) == LR_REPORT_COLD || \
	 (op) == LR_REPORT_CONNECTED || (op) == LR_REPORT_STATUS || \
	 (op) == LR_REPORT_DEDUP)

/*
 * Channel contract between LogicalSource (reader) and LogicalApply (writer)
 * ------------------------------------------------------------------------
 *
 * Every row on the Explicit Motion channel is (target_segid, relid, op,
 * payload, seq):
 *
 *   target_segid  destination segment; the Motion routes on it.  Broadcast
 *                 rows are emitted once per destination segment.
 *   relid         the *source* (remote) relation OID as it appears in the
 *                 pgoutput stream, or InvalidOid for rows that are not tied
 *                 to one relation (T carries its list inside the payload,
 *                 B/E and reports use InvalidOid).  The writer maps it to a
 *                 local relation with the relmap it builds from R rows.
 *   op            one of the LR_OP_* / LR_REPORT_* codes.
 *   payload       for data rows: the body of the pgoutput message of the
 *                 same kind, in the non-streaming form, WITHOUT the leading
 *                 type byte, i.e. exactly the bytes that proto.c's
 *                 logicalrep_read_insert / _update / _delete / _truncate /
 *                 _rel / _typ consume.  So an I payload starts with the
 *                 int32 relid followed by 'N' and the tuple; U with relid,
 *                 optional 'K'/'O' old tuple and 'N' new tuple; D with relid
 *                 and 'K'/'O' old tuple; T with nrelids, options, relids;
 *                 R and Y with the relation / type descriptions.  The
 *                 reader forwards these bytes as received (a U whose
 *                 distribution key changed is re-emitted as a D plus an I,
 *                 both re-serialized with logicalrep_write_*).
 *                 For B/E rows: an int64 in network byte order, the gxid
 *                 (B) or the segment LSN (E) of the source transaction.
 *                 For report rows: LrReportSerialize() output.
 *   seq           (source_segid << 48) | per-reader counter; strictly
 *                 increasing per (source, destination) pair.
 *
 * Ordering: the Motion is FIFO per (sender, receiver).  A row for relation
 * X is only meaningful after the R row describing X reached the same
 * writer, so the reader broadcasts R (and Y) rows before the first data row
 * of that relation in each APPLY plan, in the order the stream delivered
 * them.
 */

/*
 * Mode payloads (LrPlanSpec.payload), one "key=value" per line:
 *
 *   CONNECT  slot=<name>  pub=<p1,p2>  binary=0|1  streaming=0|1
 *            twophase=0|1  and per target segment i:
 *            conninfo.<i>=<libpq conninfo of source segment>
 *            flush.<i>=<lsn|0/0>  pos.<i>=<lsn|0/0>  applied.<i>=<g1,g2,...>
 *            srcrepl=<remote relid,...>  relations that are DISTRIBUTED
 *            REPLICATED on the source: every segment stream carries their
 *            changes, so only the reader of source segment 0 emits them.
 *   DRAIN    gxid=<G|0>  parts=<i,j,...>  wait_ms=<n>
 *            release_gxid=<g1,g2,...>  release_local=<seg>:<lsn>,...
 *            gxid=0 is the long poll: wait up to wait_ms for anything new.
 *   APPLY    txns=<seg>:<gxid>,<seg>:L<lsn>,...  in the order to apply;
 *            each reader takes the entries of its own segment.
 *            defer=<remote relid,...>    tables whose initial copy is still
 *            running: their changes are not emitted and their transactions
 *            stay buffered until a catch-up plan takes them.
 *            catchup=<remote relid,...>  the reverse: emit only these tables,
 *            which is how a table that just finished its copy is caught up.
 *
 * Debug hook of LogicalSource: a spec payload of the form
 *   inject:[<source_segid>@]<target_segid>,<relid>,<op>,<hex payload>[;<row>...]
 * makes the reader on source_segid (default 0) emit exactly those rows, in
 * order, and nothing else.  It lets the writer side be exercised without any
 * replication stream.
 */
#define LR_INJECT_PREFIX	"inject:"

/*
 * Report row: (seg int4, kind "char", gxid int8, seg_lsn int8,
 * remote_relids oid[], rel_names text[], lockmode int2, nrows int8,
 * nbytes int8).
 */
#define LR_RPT_NATTS			9
#define LR_RPT_ATT_SEG			1
#define LR_RPT_ATT_KIND			2
#define LR_RPT_ATT_GXID			3
#define LR_RPT_ATT_SEG_LSN		4
#define LR_RPT_ATT_REMOTE_RELIDS 5
#define LR_RPT_ATT_REL_NAMES	6
#define LR_RPT_ATT_LOCKMODE		7
#define LR_RPT_ATT_NROWS		8
#define LR_RPT_ATT_NBYTES		9

typedef struct LrReport
{
	int32		seg;
	char		kind;

	/*
	 * What identifies the transaction, which depends on the kind: the gxid of
	 * a READY one, the source segment's own xid of a LOCAL one (a utility
	 * write has no gxid, and the xid is what a cut point's local snapshot is
	 * stated in), and the reader's flush position of a STATUS row.
	 */
	int64		gxid;
	int64		seg_lsn;
	List	   *remote_relids;	/* list of Oid */
	List	   *rel_names;		/* list of char * */
	int16		lockmode;
	int64		nrows;
	int64		nbytes;
} LrReport;

extern const char *LrPlanModeName(LrPlanMode mode);

extern TupleDesc LrSourceTupleDesc(void);
extern TupleDesc LrReportTupleDesc(void);

extern void LrReportSerialize(StringInfo buf, const LrReport *rpt);
extern void LrReportDeserialize(const char *data, int len, LrReport *rpt);
extern void LrReportToValues(const LrReport *rpt, Datum *values, bool *isnull);

extern PlannedStmt *LrBuildApplyPlan(const LrPlanSpec *spec);
extern Tuplestorestate *LrExecuteApplyPlan(PlannedStmt *pstmt,
										   const LrPlanSpec *spec,
										   const char *sourceText,
										   MemoryContext resultcxt);
extern char *LrExplainApplyPlan(PlannedStmt *pstmt, const char *sourceText);

extern void LrRegisterCustomScanMethods(void);

/*
 * Shared between mpp_apply_plan.c and the operator files mpp_source.c /
 * mpp_writer.c.
 */
struct CustomScanMethods;
struct ExplainState;
struct EState;
struct ScanState;
struct TupleTableSlot;

extern const struct CustomScanMethods LrSourceScanMethods;
extern const struct CustomScanMethods LrApplyScanMethods;

extern void LrSpecFromCustomScan(const CustomScan *cscan, LrPlanSpec *spec);
extern bool LrSliceIsLocal(struct EState *estate);
extern void LrExplainSpec(const LrPlanSpec *spec, struct ExplainState *es);
extern struct TupleTableSlot *LrStoreReport(struct ScanState *ss,
											const LrReport *rpt);
extern bool LrScanRecheck(struct ScanState *ss, struct TupleTableSlot *slot);

/*
 * Bookkeeping for relation.c's relation map, which both operators use on
 * QEs: entries opened by an operator are recorded so that a transaction
 * abort (which frees the executor state without EndCustomScan) can mark
 * them closed again.  mpp_apply_plan.c.
 */
struct LogicalRepRelMapEntry;
extern void LrRelmapTrackOpen(struct LogicalRepRelMapEntry *entry);
extern void LrRelmapTrackClose(struct LogicalRepRelMapEntry *entry);

/*
 * Router (mpp_route.c): decides the target segment of a change from the
 * target table's distribution policy.  A change routes to
 * one segment, to every segment (target -1: the caller emits it once per
 * segment), or, for an UPDATE that moves a row, becomes a DELETE plus an
 * INSERT.
 */
struct LogicalRepRelation;
typedef struct LrRouter LrRouter;

typedef struct LrRoutedRow
{
	int			target;			/* segment, or -1 = every segment */
	char		op;
	const char *data;			/* valid until the next LrRouteChange() */
	int			len;
} LrRoutedRow;

extern LrRouter *LrRouterCreate(MemoryContext cxt, int nsegs, int source_seg);
extern int	LrRouteChange(LrRouter *r, const struct LogicalRepRelation *remoterel,
						  uint32 generation, char op, const char *body, int len,
						  LrRoutedRow out[2]);
extern void LrRouterClose(LrRouter *r);

/*
 * Initial-sync copy transport (mpp_apply_sql.c + cbdb_lr_copyhelper).
 * gp_lr_copy_recv()/gp_lr_copy_ports() are SQL functions; these GUCs tune the
 * helper's TCP listen range and per-frame checksumming.
 */
extern int	gp_lr_copy_port_lo;
extern int	gp_lr_copy_port_hi;
extern bool gp_lr_copy_crc;
extern bool gp_lr_direct_copy;
extern char *gp_lr_copy_bind_address;

#endif							/* MPP_APPLY_H */
