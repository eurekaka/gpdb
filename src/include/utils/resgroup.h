/*-------------------------------------------------------------------------
 *
 * resscheduler.h
 *	  POSTGRES resource scheduler definitions.
 *
 *
 * Copyright (c) 2006-2008, Greenplum inc.
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  $PostgreSQL: $
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESGROUP_H
#define RESGROUP_H

//#include "storage/lock.h"
//#include "cdb/cdbvars.h"

/* GUC */
extern int MaxResourceGroups;
extern Oid	CurrentGroupId;

/*
 * Data structures
 */

/* Resource Groups */
typedef struct ResGroupData
{
	Oid			groupId;		/* Id for this group */
	int			concurrency;	/* current concurrency of this group*/
	PROC_QUEUE	waitProcs;		/* Queueing procs of this group */
} ResGroupData;
typedef ResGroupData *ResGroup;

/*
 * We must have a limit on max resource groups created, to prevent explosion of
 * shared memory, so we have to track the number of created groups.
 *
 * The hash table for resource groups in shared memory should only be populated
 * once, so we add a flag here to implement this requirement.
 */
typedef struct ResGroupControl
{
	HTAB 	*htbl;		/* Hash table of ResGroupData */
	bool	htblLoaded; /* htbl is initialized? */
} ResGroupControl;

/*
 * Functions in resgroup.c
 */
extern void ResGroupSlotAcquire(Oid groupId);
extern void ResGroupSlotRelease(Oid groupId);

extern void ResGroupControlInit(void);

extern void	InitResGroups(void);
extern bool ResGroupCreate(Oid groupId);

extern void SetResGroupId(void);
extern Oid	GetResGroupId(void);

extern Size ResGroupShmemSize(void);

#endif   /* RESGROUP_H */
