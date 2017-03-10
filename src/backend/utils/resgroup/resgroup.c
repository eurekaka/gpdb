/*-------------------------------------------------------------------------
 *
 * resgroup.c
 *	  Greenplum internals code for resource groups.
 *
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>

#include "pgstat.h"
#include "access/heapam.h"
#include "access/genam.h"
#include "access/twophase.h"
#include "access/twophase_rmgr.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_type.h"
#include "catalog/pg_resgroup.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/syscache.h"
#include "utils/resowner.h"
#include "utils/resgroup.h"
#include "cdb/memquota.h"

/* GUC */
int						MaxResourceGroups;

static ResGroupControl *pResGroupControl;
static Oid				MyGroupId;
Oid						CurrentGroupId;

Datum
pg_resgroup_get_status_kv(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;
		TupleDesc	tupdesc;
		int			nattr = 3;

		funcctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		tupdesc = CreateTemplateTupleDesc(nattr, false);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "rsgid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "prop", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "value", TEXTOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		if (PG_ARGISNULL(0))
		{
			funcctx->max_calls = 0;
		}
		else
		{
			/* dummy output */
			funcctx->max_calls = 2;
			funcctx->user_fctx = palloc0(sizeof(Datum) * funcctx->max_calls);
			((Datum *) funcctx->user_fctx)[0] = ObjectIdGetDatum(6437);
			((Datum *) funcctx->user_fctx)[1] = ObjectIdGetDatum(6438);
		}

		MemoryContextSwitchTo(oldcontext);
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		/* for each row */
		char *		prop = text_to_cstring(PG_GETARG_TEXT_P(0));
		Datum		values[3];
		bool		nulls[3];
		HeapTuple	tuple;

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = ((Datum *) funcctx->user_fctx)[funcctx->call_cntr];
		values[1] = CStringGetTextDatum(prop);

		/* Fill with dummy values */
		if (!strcmp(prop, "num_running"))
			values[2] = CStringGetTextDatum("1");
		else if (!strcmp(prop, "num_grouping"))
			values[2] = CStringGetTextDatum("0");
		else if (!strcmp(prop, "cpu_usage"))
			values[2] = CStringGetTextDatum("0.0");
		else if (!strcmp(prop, "memory_usage"))
			values[2] = CStringGetTextDatum("0.0");
		else if (!strcmp(prop, "total_group_duration"))
			values[2] = CStringGetTextDatum("00:00:00");
		else if (!strcmp(prop, "num_groupd"))
			values[2] = CStringGetTextDatum("0");
		else if (!strcmp(prop, "num_executed"))
			values[2] = CStringGetTextDatum("0");
		else
			/* unknown property name */
			nulls[2] = true;

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * ResGroupControlInit -- initialize the global ResGroupControl struct of resource groups.
 */
void
ResGroupControlInit(void)
{
	bool		found;
	HASHCTL		info;
	int			hash_flags;

	pResGroupControl = ShmemInitStruct("global resource group control",
									   sizeof(*pResGroupControl), &found);
	if (found)
		return;
	if (pResGroupControl == NULL)
		goto error_out;

	/* Set key and entry sizes of hash table */
	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ResGroupData);
	info.hash = tag_hash;

	hash_flags = (HASH_ELEM | HASH_FUNCTION);

	ELOG_RESGROUP_DEBUG("Creating hash table for %d resource groups", MaxResourceGroups);

	pResGroupControl->htbl = ShmemInitHash("Resource Group Hash Table",
										   MaxResourceGroups,
										   MaxResourceGroups,
										   &info, hash_flags);

	if (!pResGroupControl->htbl)
		goto error_out;

	/*
	 * No need to acquire LWLock here, since this is expected to be called by
	 * postmaster only
	 */
	pResGroupControl->htblLoaded = false;
	return;

error_out:
	ereport(FATAL,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("not enough shared memory for resource group control")));

}

/*
 * ResGroupHashNew -- return a new (empty) group object to initialize.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroup
ResGroupHashNew(Oid groupId)
{
	bool		found;
	ResGroup	group = NULL;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	group = (ResGroup)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_ENTER_NULL, &found);

	/* caller should test that the group does not exist already */
	Assert(!found);

	return group;
}

/*
 * ResGroupHashFind -- return the group for a given oid.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static ResGroup
ResGroupHashFind(Oid groupId)
{
	bool		found;
	ResGroup	group = NULL;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	group = (ResGroup)
		hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_FIND, &found);

	return group;
}


/*
 * ResGroupHashRemove -- remove the group for a given oid.
 *
 * Notes
 *	The resource group lightweight lock (ResGroupLock) *must* be held for
 *	this operation.
 */
static bool
ResGroupHashRemove(Oid groupId)
{
	bool		found;
	void	   *group;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));

	group = hash_search(pResGroupControl->htbl, (void *) &groupId, HASH_REMOVE, &found);
	if (!group)
		return false;

	return true;
}

/*
 * InitResGroups -- initialize the resource groups in shared memory. Note this
 * can only be done after enough setup has been done. This uses
 * heap_open etc which in turn requires shared memory to be set up.
 */
void 
InitResGroups(void)
{
	HeapTuple	tuple;
	int			numGroups;
	SysScanDesc	sscan;

	if (pResGroupControl->htblLoaded)
		return;
	
	/*
	 * Need a resource owner to keep the heapam code happy.
	 */
	Assert(CurrentResourceOwner == NULL);
	ResourceOwner owner = ResourceOwnerCreate(NULL, "InitGroups");
	CurrentResourceOwner = owner;
	
	/*
	 * The resgroup shared mem initialization must be serialized. Only the first session
	 * should do the init.
	 * Serialization is done by LW_EXCLUSIVE ResGroupLock. However, we must obtain all DB
	 * locks before obtaining LWlock to prevent deadlock. 
	 */
	Relation relResGroup = heap_open(ResGroupRelationId, AccessShareLock);
	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);

	if (pResGroupControl->htblLoaded)
		goto exit;

	numGroups = 0;
	sscan = systable_beginscan(relResGroup, InvalidOid, false, SnapshotNow, 0, NULL);
	while (HeapTupleIsValid(tuple = systable_getnext(sscan)))
	{
		bool groupsOK = ResGroupCreate(HeapTupleGetOid(tuple));

		if (!groupsOK)
			ereport(PANIC,
					(errcode(ERRCODE_OUT_OF_MEMORY),
			 		errmsg("not enough shared memory for resource groups")));

		numGroups++;
		Assert(numGroups <= MaxResourceGroups);
	}
	systable_endscan(sscan);
	
	/* XXX: do we need a write barrier here? */
	pResGroupControl->htblLoaded = true;
	ELOG_RESGROUP_DEBUG("initialized %d resource groups", numGroups);

exit:
	LWLockRelease(ResGroupLock);
	heap_close(relResGroup, AccessShareLock);
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(owner);
}

/*
 * ResGroupCreate -- initialize the elements for a resource group.
 *
 * Notes:
 *	It is expected that the appropriate lightweight lock is held before
 *	calling this - unless we are the startup process.
 */
bool
ResGroupCreate(Oid groupId)
{
	ResGroup		group;
	int				i;

	Assert(LWLockHeldExclusiveByMe(ResGroupLock));
	Assert(OidIsValid(groupId));

	group = ResGroupHashNew(groupId);
	if (group == NULL)
		return false;

	group->groupId = groupId;
	group->concurrency = 0;
	ProcQueueInit(&group->waitProcs);

	return true;
}

static void
ResWaitOnSem(ResGroup group)
{
	PGPROC *proc = MyProc, *headProc;
	PROC_QUEUE *waitQueue;
	int extraWaits = 0;

	proc->resWaiting = true;

	waitQueue = &(group->waitProcs);

	headProc = (PGPROC *) &(waitQueue->links);
	SHMQueueInsertBefore(&(headProc->links), &(proc->links));
	waitQueue->size++;

	LWLockRelease(ResGroupLock);

	for (;;)
	{
		PGSemaphoreLock(&proc->sem, false);

		if (!proc->resWaiting)
			break;
		extraWaits ++;
	}

	while (extraWaits-- > 0)
		PGSemaphoreUnlock(&proc->sem);	
}

void
ResGroupSlotAcquire(Oid groupId)
{
	SysScanDesc	sscan;
	ScanKeyData	key[2];
	HeapTuple	tuple;
	char		*valueStr;
	int			concurrencyLimit;
	ResGroup	group;
	Form_pg_resgroupcapability	capability;

	if (groupId == ADMINRESGROUP_OID)
		return;

	/*
	 * to cave the code of cache part, we provide a resource owner here if no
	 * existing
	 */
	ResourceOwner owner = NULL;

	if (CurrentResourceOwner == NULL)
	{
		owner = ResourceOwnerCreate(NULL, "ResGroupSlotAcquire");
		CurrentResourceOwner = owner;
	}

	Relation relResGroupCapability = heap_open(ResGroupCapabilityRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_resgroupcapability_resgroupid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(groupId));
	ScanKeyInit(&key[1],
				Anum_pg_resgroupcapability_reslimittype,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(RESGROUP_LIMIT_TYPE_CONCURRENCY));

	sscan = systable_beginscan(relResGroupCapability,
							   ResGroupCapabilityResgroupidResLimittypeIndexId,
							   true,
							   SnapshotNow, 2, key);

	tuple = systable_getnext(sscan);
	if (!HeapTupleIsValid(tuple))
	{
		systable_endscan(sscan);
		heap_close(relResGroupCapability, AccessShareLock);

		if (owner)
		{
			CurrentResourceOwner = NULL;
			ResourceOwnerDelete(owner);
		}

		/*TODO error code*/
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
			 	 errmsg("Invalid resource group id:%d", groupId)));
	}

	capability = (Form_pg_resgroupcapability) GETSTRUCT(tuple);
	// TODO: text convert
	valueStr = DatumGetCString(DirectFunctionCall1(
									textout,
									PointerGetDatum(&capability->value)));
	/*TODO atoi*/
	concurrencyLimit= atoi(valueStr);
	systable_endscan(sscan);
	heap_close(relResGroupCapability, AccessShareLock);

	if (owner)
	{
		CurrentResourceOwner = NULL;
		ResourceOwnerDelete(owner);
	}

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
	group = ResGroupHashFind(groupId);

	/*TODO error code*/
	if (group == NULL)
	{
		LWLockRelease(ResGroupLock);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
			 	 errmsg("Cannot find resource group %d in shared memory", groupId)));
	}
	/*TODO change to per-group lock*/
	
	if (group->concurrency < concurrencyLimit) 
	{
		group->concurrency ++;
		LWLockRelease(ResGroupLock);
		return;
	}

	ResWaitOnSem(group);
}

void
ResGroupSlotRelease(Oid groupId)
{
	ResGroup	group;
	PROC_QUEUE	*waitQueue;
	PGPROC		*waitProc;

	if (groupId == ADMINRESGROUP_OID)
		return;

	LWLockAcquire(ResGroupLock, LW_EXCLUSIVE);
	group = ResGroupHashFind(groupId);
	waitQueue = &(group->waitProcs);
	
	if (waitQueue->size == 0)
	{
		Assert(waitQueue->links.next == MAKE_OFFSET(&waitQueue->links) &&
			   waitQueue->links.prev == MAKE_OFFSET(&waitQueue->links));

		if (group->concurrency == 0)
		{
			elog(PANIC, "aaa");
		}

		group->concurrency --;
		LWLockRelease(ResGroupLock);
		return;
	}

	/* wake up one process in the wait queue */
	waitProc = (PGPROC *) MAKE_PTR(waitQueue->links.next);
	SHMQueueDelete(&(waitProc->links));
	waitQueue->size --;
	LWLockRelease(ResGroupLock);

	waitProc->resWaiting = false;
	PGSemaphoreUnlock(&waitProc->sem);
}

/*
 * GetResGroupForRole -- determine what resource group a role is going to use.
 */
static Oid	
GetResGroupForRole(Oid roleid)
{
	HeapTuple	tuple;
	bool		isnull;
	Oid			groupId;

	tuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(roleid));
	if (!tuple)
		return DEFAULTRESGROUP_OID; /* role not found */

	groupId = SysCacheGetAttr(AUTHOID, tuple, Anum_pg_authid_rolresgroup, &isnull);

	if (!OidIsValid(groupId) || isnull)
		groupId = DEFAULTRESGROUP_OID;

	ReleaseSysCache(tuple);

	return groupId;
}

void
SetResGroupId(void)
{
	/* to cave the code of cache part, we provide a resource owner here if no
	 * existing */
	ResourceOwner owner = NULL;

	if (CurrentResourceOwner == NULL)
	{
		owner = ResourceOwnerCreate(NULL, "SetResGroupId");
		CurrentResourceOwner = owner;
	}

	MyGroupId = GetResGroupForRole(GetUserId());

	if (owner)
	{
		CurrentResourceOwner = NULL;
		ResourceOwnerDelete(owner);
	}

	return;
}

Oid
GetResGroupId(void)
{
	return MyGroupId;
}

/*
 * ResGroupShmemSize -- estimate size the resource group structures will need in
 *	shared memory.
 */
Size
ResGroupShmemSize(void)
{
	Size		size;

	/* The hash of groups. */
	size = hash_estimate_size(MaxResourceGroups, sizeof(ResGroupData));

	/* The control structure. */
	size = add_size(size, sizeof(ResGroupControl));

	/* Add a safety margin */
	size = add_size(size, size / 10);

	return size;
}
