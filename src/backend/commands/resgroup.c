/*-------------------------------------------------------------------------
 *
 * resgroup.c
 *	  Commands for manipulating resource groups.
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 * IDENTIFICATION
 *    src/backend/commands/resgroup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_resgroup.h"
#include "nodes/makefuncs.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "commands/resgroup.h"
#include "libpq/crypt.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/flatfiles.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "executor/execdesc.h"
#include "utils/resscheduler.h"
#include "utils/syscache.h"
#include "cdb/memquota.h"
#include "utils/guc_tables.h"


/*
 * DROP RESOURCE GROUP
 */
void
DropResGroup(DropResGroupStmt *stmt)
{
	Relation	 pg_resgroup_rel;
	Relation	 authIdRel;
	Relation	 resgroupCapabilityRel;
	HeapTuple	 tuple;
	ScanKeyData	 scankey;
	SysScanDesc	 sscan;
	ScanKeyData	 authid_scankey;
	SysScanDesc	 authid_scan;
	Oid			 rsgid;
	bool		 dropped = false;


	/* Permission check - only superuser can drop resgroups. */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to drop resource groups")));

	/*
	 * Check the pg_resgroup relation to be certain the resgroup already
	 * exists.
	 *
	 * SELECT oid FROM pg_resgroup WHERE rsgname = :1
	 */
	/* TODO: double check whether RowExclusiveLock is sufficient */
	pg_resgroup_rel = heap_open(ResGroupRelationId, RowExclusiveLock);

	/*
	 * Get database locks in anticipation that we'll need to access this catalog table later.
	 */
	resgroupCapabilityRel = heap_open(ResGroupCapabilityRelationId, RowExclusiveLock);

	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(stmt->resgroup));

	sscan = systable_beginscan(pg_resgroup_rel, ResGroupRsgnameIndexId, true,
							   SnapshotNow, 1, &scankey);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("resource group \"%s\" does not exist",
						stmt->resgroup)));

	/*
	 * Remember the Oid, for destroying the in-memory resgroup later.
	 */
	rsgid = HeapTupleGetOid(tuple);

	/* cannot DROP default resgroups  */
	if (rsgid == DEFAULTRESGROUP_OID || rsgid == ADMINRESGROUP_OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop default resource group \"%s\"",
						stmt->resgroup)));

	/*
	 * Check to see if any roles are in this resgroup.
	 *
	 * SELECT oid FROM pg_authid WHERE rolresgroup = :1
	 */
	authIdRel = heap_open(AuthIdRelationId, RowExclusiveLock);
	ScanKeyInit(&authid_scankey,
				Anum_pg_authid_rolresgroup,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(rsgid));

	authid_scan = systable_beginscan(authIdRel, AuthIdRolResGroupIndexId, true,
							   SnapshotNow, 1, &authid_scankey);

	if (systable_getnext(authid_scan) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg("resource group \"%s\" is used by at least one role",
						stmt->resgroup)));

	systable_endscan(authid_scan);
	heap_close(authIdRel, RowExclusiveLock);

	/*
	 * Delete the resgroup from the catalog.
	 */
	simple_heap_delete(pg_resgroup_rel, &tuple->t_self);

	systable_endscan(sscan);

	/*
	 * If resource scheduling is on, see if we can destroy the in-memory resgroup.
	 * otherwise don't - and gripe a little about it.
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		if (ResourceScheduler)
		{
			LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

#if 0
			dropped = ResDestroyGroup(rsgid);
#else
			/* FIXME: dummy code */
			dropped = true;
#endif

			/*
			 * Ensure that the shared data structures are consistent with
			 * the catalog table on memory limits.
			 */
#ifdef USE_ASSERT_CHECKING
			AssertMemoryLimitsMatch();
#endif

			LWLockRelease(ResGroupLock);

			if (!dropped)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						 errmsg("resource group cannot be dropped as is in use")));
		}
		else
		{
			ereport(WARNING,
					(errmsg("resource scheduling is disabled"),
					 errhint("To enable set resource_scheduler=on")));
		}
	}

	/*
	 * Remove any comments on this resgroup
	 */
	/* TODO: implement the COMMENT ON syntax */
	DeleteSharedComments(rsgid, ResGroupRelationId);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		CdbDispatchUtilityStatement((Node *) stmt,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									NIL, /* FIXME */
									NULL);
	}
	/* metadata tracking */
	MetaTrackDropObject(ResGroupRelationId, rsgid);

	/* drop the extended attributes for this group */
	/* DELETE FROM pg_resgroupcapability WHERE resgroupid = :1 */
	ScanKeyInit(&scankey,
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(rsgid));

	sscan = systable_beginscan(resgroupCapabilityRel, ResGroupCapabilityResgroupidIndexId,
							   true, SnapshotNow, 1, &scankey);

	while ((tuple = systable_getnext(sscan)) != NULL)
		simple_heap_delete(resgroupCapabilityRel, &tuple->t_self);

	systable_endscan(sscan);


	heap_close(resgroupCapabilityRel, NoLock);

	heap_close(pg_resgroup_rel, NoLock);
}
