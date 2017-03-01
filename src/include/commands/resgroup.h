/*-------------------------------------------------------------------------
 *
 * resgroup.h
 *	  Commands for manipulating resgroups.
 *
 * Copyright (c) 2006-2017, Greenplum inc.
 *
 *-------------------------------------------------------------------------
 */
#ifndef RESGROUP_H
#define RESGROUP_H

#include "nodes/parsenodes.h"


extern void DropResGroup(DropResGroupStmt *stmt);

#endif   /* RESGROUP_H */
