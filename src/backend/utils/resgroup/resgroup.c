/*-------------------------------------------------------------------------
 *
 * resgroup.c
 *	  GPDB resource group management code.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/pg_resgroup.h"
#include "utils/fmgroids.h"
#include "utils/resgroup.h"

/*
 * GetResGroupIdForName -- Return the Oid for a resource group name
 *
 * Notes:
 *	Used by the various admin commands to convert a user supplied group name
 *	to Oid.
 */
Oid
GetResGroupIdForName(char *name, LOCKMODE lockmode)
{
	Relation	rel;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			rsgid;

	rel = heap_open(ResGroupRelationId, lockmode);

	/* SELECT oid FROM pg_resgroup WHERE rsgname = :1 */
	ScanKeyInit(&scankey,
				Anum_pg_resgroup_rsgname,
				BTEqualStrategyNumber, F_NAMEEQ,
				CStringGetDatum(name));
	scan = systable_beginscan(rel, ResGroupRsgnameIndexId, true,
							  SnapshotNow, 1, &scankey);

	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
		rsgid = HeapTupleGetOid(tuple);
	else
		rsgid = InvalidOid;

	systable_endscan(scan);
	heap_close(rel, lockmode);

	return rsgid;
}
