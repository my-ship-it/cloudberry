/*-------------------------------------------------------------------------
 *
 * mpp_route.c
 *	  Routing of logical replication changes to target segments.
 *
 * The reader QE decides, for every change it emits on the Explicit Motion
 * channel, which target segment must apply it.  The decision follows the
 * target table's distribution policy:
 *
 *   hash(keys)   INSERT      cdbhash(new keys)
 *                DELETE      cdbhash(old keys)
 *                UPDATE      same segment when the keys did not move; a
 *                            DELETE to the old plus an INSERT to the new
 *                            segment when they did (or when the old segment
 *                            cannot be computed: DELETE to every segment)
 *
 * An UPDATE whose old tuple is absent is taken to leave the row where it is.
 * That rests on an invariant worth stating, because the rest of the routing
 * would silently lose such a row without it: the source only omits the old
 * tuple when the replica identity did not change, and the target's
 * distribution key is always part of that identity -- Cloudberry requires
 * every unique index to contain the distribution key, so a target with an
 * identity index has the distribution key inside it, and a target without
 * one can only be applied to when the source publishes REPLICA IDENTITY FULL
 * (logicalrep_rel_mark_updatable), which sends the old tuple every time.
 * route_rel_open() checks that the invariant holds for each relation rather
 * than trusting it.
 *   replicated   everything to every segment
 *   randomly     INSERT stays on the reader's own segment; UPDATE and DELETE
 *                go to every segment, only one of which finds the row
 *
 * Only the distribution key columns are decoded, with the target column's
 * input (or receive) function; the rest of the tuple is forwarded as the
 * bytes that arrived.  The target relation is resolved by name through
 * relation.c's relation map, like the writer does, which also validates
 * that every replicated column exists locally.
 *
 * Portions Copyright (c) 2026, Apache Cloudberry
 *
 * IDENTIFICATION
 *	  contrib/mpp_logicalrep/mpp_route.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/gp_distribution_policy.h"
#include "cdb/cdbhash.h"
#include "libpq/pqformat.h"
#include "replication/logicalproto.h"
#include "replication/logicalrelation.h"
#include "mpp_apply.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

typedef enum LrDistKind
{
	LR_DIST_HASH,
	LR_DIST_REPLICATED,
	LR_DIST_RANDOM,
} LrDistKind;

/* one target relation, open for the lifetime of the router */
typedef struct LrRouteRel
{
	Oid			remoteid;		/* hash key */
	uint32		generation;		/* of the description this was built from */
	LogicalRepRelMapEntry *entry;	/* open, AccessShareLock */
	LrDistKind	kind;
	int			nkeys;
	bool		key_in_identity;	/* the source sends the old distribution
									 * key whenever it changes */
	int		   *remote_attnum;	/* per key: column in the remote tuple */
	Oid		   *typinput;
	Oid		   *typreceive;
	Oid		   *typioparam;
	int32	   *typmod;
	CdbHash    *hash;
} LrRouteRel;

struct LrRouter
{
	MemoryContext cxt;			/* relations, hash states */
	MemoryContext rowcxt;		/* reset per change */
	int			nsegs;
	int			source_seg;
	HTAB	   *rels;
};

#define LR_TARGET_ALL		(-1)
#define LR_TARGET_UNKNOWN	(-2)	/* key not computable from the tuple */

static LrRouteRel *route_rel_open(LrRouter *r, const LogicalRepRelation *remoterel,
								  uint32 generation);
static void route_rel_close(LrRouter *r, LrRouteRel *rr);
static int	route_tuple(LrRouter *r, LrRouteRel *rr, const LogicalRepTupleData *tup,
						bool key_only_tuple);
static int	split_update(const char *body, int len, int *old_end);

LrRouter *
LrRouterCreate(MemoryContext cxt, int nsegs, int source_seg)
{
	LrRouter   *r;
	HASHCTL		ctl;
	MemoryContext oldcxt = MemoryContextSwitchTo(cxt);

	r = palloc0(sizeof(LrRouter));
	r->cxt = cxt;
	r->rowcxt = AllocSetContextCreate(cxt, "LrRouter row", ALLOCSET_DEFAULT_SIZES);
	r->nsegs = nsegs;
	r->source_seg = source_seg;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(LrRouteRel);
	ctl.hcxt = cxt;
	r->rels = hash_create("LrRouter relations", 16, &ctl,
						  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	MemoryContextSwitchTo(oldcxt);
	return r;
}

void
LrRouterClose(LrRouter *r)
{
	HASH_SEQ_STATUS seq;
	LrRouteRel *rr;
	List	   *all = NIL;
	ListCell   *lc;

	hash_seq_init(&seq, r->rels);
	while ((rr = (LrRouteRel *) hash_seq_search(&seq)) != NULL)
		all = lappend(all, rr);
	foreach(lc, all)
		route_rel_close(r, (LrRouteRel *) lfirst(lc));
	list_free(all);
}

/*
 * Route one change.  Fills out[] with 1 or 2 rows and returns their count.
 */
int
LrRouteChange(LrRouter *r, const LogicalRepRelation *remoterel,
			  uint32 generation, char op, const char *body, int len,
			  LrRoutedRow out[2])
{
	LrRouteRel *rr;
	MemoryContext oldcxt;
	StringInfoData s;
	int			n = 0;

	rr = (LrRouteRel *) hash_search(r->rels, &remoterel->remoteid, HASH_FIND,
									NULL);
	if (rr && rr->generation != generation)
	{
		route_rel_close(r, rr);
		rr = NULL;
	}
	if (rr == NULL)
		rr = route_rel_open(r, remoterel, generation);

	MemoryContextReset(r->rowcxt);
	oldcxt = MemoryContextSwitchTo(r->rowcxt);

	/* NUL-terminated private copy for the proto.c readers */
	s.data = palloc(len + 1);
	memcpy(s.data, body, len);
	s.data[len] = '\0';
	s.len = len;
	s.maxlen = len + 1;
	s.cursor = 0;

	out[0].op = op;
	out[0].data = body;
	out[0].len = len;

	switch (op)
	{
		case LR_OP_INSERT:
			{
				LogicalRepTupleData newtup;

				(void) logicalrep_read_insert(&s, &newtup);
				switch (rr->kind)
				{
					case LR_DIST_REPLICATED:
						out[0].target = LR_TARGET_ALL;
						break;
					case LR_DIST_RANDOM:
						out[0].target = r->source_seg % r->nsegs;
						break;
					case LR_DIST_HASH:
						out[0].target = route_tuple(r, rr, &newtup, false);
						if (out[0].target == LR_TARGET_UNKNOWN)
							ereport(ERROR,
									(errcode(ERRCODE_PROTOCOL_VIOLATION),
									 errmsg("cannot route INSERT into \"%s.%s\": distribution key column not present",
											remoterel->nspname, remoterel->relname)));
						break;
				}
				n = 1;
				break;
			}

		case LR_OP_DELETE:
			{
				LogicalRepTupleData oldtup;
				bool		key_only = (len > 4 && body[4] == 'K');

				(void) logicalrep_read_delete(&s, &oldtup);
				if (rr->kind == LR_DIST_HASH)
				{
					out[0].target = route_tuple(r, rr, &oldtup, key_only);
					if (out[0].target == LR_TARGET_UNKNOWN)
						out[0].target = LR_TARGET_ALL;
				}
				else
					out[0].target = LR_TARGET_ALL;
				n = 1;
				break;
			}

		case LR_OP_UPDATE:
			{
				LogicalRepTupleData oldtup;
				LogicalRepTupleData newtup;
				bool		has_old;
				bool		key_only = (len > 4 && body[4] == 'K');
				int			old_t,
							new_t;

				(void) logicalrep_read_update(&s, &has_old, &oldtup, &newtup);

				if (rr->kind != LR_DIST_HASH)
				{
					/* replicated: everywhere; random: only one segment has it */
					out[0].target = LR_TARGET_ALL;
					n = 1;
					break;
				}

				new_t = route_tuple(r, rr, &newtup, false);

				/*
				 * No old tuple: the replica identity did not change, and the
				 * distribution key is part of it, so the row does not move.
				 * If it is not part of it the old segment is simply not in
				 * the stream -- applying the UPDATE anywhere would lose the
				 * row instead of moving it, so say so.
				 */
				if (!has_old && !rr->key_in_identity)
					ereport(ERROR,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("cannot apply UPDATE to \"%s.%s\": its distribution key is not part of the replica identity of the source relation",
									remoterel->nspname, remoterel->relname),
							 errdetail("The row may have to move to another segment of the target, which needs the old value of the distribution key; the source does not send it."),
							 errhint("Set REPLICA IDENTITY FULL on the source relation, or distribute the target relation by columns of its replica identity.")));

				old_t = has_old ? route_tuple(r, rr, &oldtup, key_only) : new_t;
				if (new_t == LR_TARGET_UNKNOWN)
				{
					/*
					 * An unchanged toasted key column is not sent in the new
					 * tuple; the key did not move then, so the old tuple
					 * (which heap logs whenever a key column is external)
					 * tells the segment.
					 */
					if (has_old && old_t >= 0)
						new_t = old_t;
					else
						ereport(ERROR,
								(errcode(ERRCODE_PROTOCOL_VIOLATION),
								 errmsg("cannot route UPDATE of \"%s.%s\": distribution key column not present in the new tuple",
										remoterel->nspname, remoterel->relname)));
				}

				if (old_t == new_t)
				{
					out[0].target = new_t;
					n = 1;
				}
				else
				{
					/*
					 * The row moves, or its old segment cannot be computed:
					 * DELETE the old row where it is (everywhere if unknown)
					 * and INSERT the new one where it belongs.  Both bodies
					 * reuse the bytes of the UPDATE message.
					 */
					int			old_end;
					char	   *dbody,
							   *ibody;

					if (!split_update(body, len, &old_end))
						elog(ERROR, "malformed UPDATE message");

					dbody = palloc(old_end);
					memcpy(dbody, body, old_end);	/* relid + K/O + old tuple */
					ibody = palloc(4 + (len - old_end));
					memcpy(ibody, body, 4);	/* relid */
					memcpy(ibody + 4, body + old_end, len - old_end);	/* N + new */

					out[0].op = LR_OP_DELETE;
					out[0].target = old_t == LR_TARGET_UNKNOWN ? LR_TARGET_ALL : old_t;
					out[0].data = dbody;
					out[0].len = old_end;
					out[1].op = LR_OP_INSERT;
					out[1].target = new_t;
					out[1].data = ibody;
					out[1].len = 4 + (len - old_end);
					n = 2;
				}
				break;
			}

		default:
			elog(ERROR, "LrRouteChange: op '%c' is not a routable change", op);
	}

	MemoryContextSwitchTo(oldcxt);
	return n;
}

/*
 * Offset in an UPDATE body where the 'N' of the new tuple starts, i.e. the
 * end of the old tuple part.  The body must carry an old tuple.
 */
static int
split_update(const char *body, int len, int *old_end)
{
	StringInfoData s;
	LogicalRepTupleData oldtup;

	if (len <= 5 || (body[4] != 'K' && body[4] != 'O'))
		return 0;

	/* relid + K/O + tuple is exactly what a DELETE body looks like */
	s.data = (char *) body;
	s.len = len;
	s.maxlen = -1;
	s.cursor = 0;
	(void) logicalrep_read_delete(&s, &oldtup);
	if (s.cursor >= len || body[s.cursor] != 'N')
		return 0;
	*old_end = s.cursor;
	return 1;
}

/*
 * Segment for a tuple by the target's hash policy.  key_only_tuple says the
 * tuple is a 'K' old tuple, where a NULL column may just be "not part of the
 * replica identity" and the key may therefore be unknown.
 */
static int
route_tuple(LrRouter *r, LrRouteRel *rr, const LogicalRepTupleData *tup,
			bool key_only_tuple)
{
	cdbhashinit(rr->hash);
	for (int i = 0; i < rr->nkeys; i++)
	{
		int			ra = rr->remote_attnum[i];
		StringInfo	colvalue;
		Datum		d = (Datum) 0;
		bool		isnull = false;

		if (ra >= tup->ncols)
			return LR_TARGET_UNKNOWN;
		colvalue = &tup->colvalues[ra];
		switch (tup->colstatus[ra])
		{
			case LOGICALREP_COLUMN_TEXT:
				d = OidInputFunctionCall(rr->typinput[i], colvalue->data,
										 rr->typioparam[i], rr->typmod[i]);
				break;
			case LOGICALREP_COLUMN_BINARY:
				colvalue->cursor = 0;
				d = OidReceiveFunctionCall(rr->typreceive[i], colvalue,
										   rr->typioparam[i], rr->typmod[i]);
				break;
			case LOGICALREP_COLUMN_NULL:
				if (key_only_tuple)
					return LR_TARGET_UNKNOWN;
				isnull = true;
				break;
			case LOGICALREP_COLUMN_UNCHANGED:
			default:
				return LR_TARGET_UNKNOWN;
		}
		cdbhash(rr->hash, i + 1, d, isnull);
	}
	return (int) cdbhashreduce(rr->hash);
}

static LrRouteRel *
route_rel_open(LrRouter *r, const LogicalRepRelation *remoterel,
			   uint32 generation)
{
	LrRouteRel *rr;
	bool		found;
	MemoryContext oldcxt;
	Relation	localrel;
	GpPolicy   *policy;
	TupleDesc	desc;

	rr = (LrRouteRel *) hash_search(r->rels, &remoterel->remoteid, HASH_ENTER,
									&found);
	Assert(!found);
	memset(rr, 0, sizeof(LrRouteRel));
	rr->remoteid = remoterel->remoteid;
	rr->generation = generation;

	oldcxt = MemoryContextSwitchTo(r->cxt);

	/* relation.c resolves the name and maps the columns */
	logicalrep_relmap_update((LogicalRepRelation *) remoterel);
	rr->entry = logicalrep_rel_open(remoterel->remoteid, AccessShareLock);
	LrRelmapTrackOpen(rr->entry);
	localrel = rr->entry->localrel;
	desc = RelationGetDescr(localrel);

	policy = localrel->rd_cdbpolicy;
	if (policy == NULL || policy->ptype == POLICYTYPE_ENTRY)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("logical replication target relation \"%s.%s\" is not distributed",
						remoterel->nspname, remoterel->relname)));

	if (policy->ptype == POLICYTYPE_REPLICATED)
		rr->kind = LR_DIST_REPLICATED;
	else if (policy->nattrs == 0)
		rr->kind = LR_DIST_RANDOM;
	else
	{
		rr->kind = LR_DIST_HASH;
		rr->nkeys = policy->nattrs;
		rr->remote_attnum = palloc(sizeof(int) * rr->nkeys);
		rr->typinput = palloc(sizeof(Oid) * rr->nkeys);
		rr->typreceive = palloc(sizeof(Oid) * rr->nkeys);
		rr->typioparam = palloc(sizeof(Oid) * rr->nkeys);
		rr->typmod = palloc(sizeof(int32) * rr->nkeys);
		for (int i = 0; i < rr->nkeys; i++)
		{
			AttrNumber	la = policy->attrs[i];
			Form_pg_attribute att = TupleDescAttr(desc, la - 1);
			Oid			dummy;

			rr->remote_attnum[i] = rr->entry->attrmap->attnums[la - 1];
			if (rr->remote_attnum[i] < 0)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("distribution key column \"%s\" of \"%s.%s\" is not replicated",
								NameStr(att->attname), remoterel->nspname,
								remoterel->relname)));
			getTypeInputInfo(att->atttypid, &rr->typinput[i], &rr->typioparam[i]);
			getTypeBinaryInputInfo(att->atttypid, &rr->typreceive[i], &dummy);
			rr->typmod[i] = att->atttypmod;
		}
		rr->hash = makeCdbHashForRelation(localrel);

		/*
		 * Can the old distribution key always be known?  With REPLICA
		 * IDENTITY FULL the source sends the whole old tuple; otherwise it
		 * sends the old key only when that key changed, so the distribution
		 * key must be part of it.
		 */
		rr->key_in_identity = (remoterel->replident == REPLICA_IDENTITY_FULL);
		if (!rr->key_in_identity)
		{
			rr->key_in_identity = true;
			for (int i = 0; i < rr->nkeys; i++)
				if (!bms_is_member(rr->remote_attnum[i], remoterel->attkeys))
					rr->key_in_identity = false;
		}
	}

	MemoryContextSwitchTo(oldcxt);
	return rr;
}

static void
route_rel_close(LrRouter *r, LrRouteRel *rr)
{
	if (rr->hash)
		freeCdbHash(rr->hash);
	logicalrep_rel_close(rr->entry, NoLock);
	LrRelmapTrackClose(rr->entry);
	hash_search(r->rels, &rr->remoteid, HASH_REMOVE, NULL);
}
