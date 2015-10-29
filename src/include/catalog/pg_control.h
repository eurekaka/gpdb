/*-------------------------------------------------------------------------
 *
 * pg_control.h
 *	  The system control file "pg_control" is not a heap relation.
 *	  However, we define it here so that the format is documented.
 *
 *
 * Portions Copyright (c) 1996-2008, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/catalog/pg_control.h,v 1.35 2007/01/05 22:19:52 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CONTROL_H
#define PG_CONTROL_H

#include <time.h>

#include "access/xlogdefs.h"
#include "port/pg_crc32c.h"


/*
 * Version identifier for this pg_control format.
 *
 * The first three digits is the PostgreSQL version number. The last
 * four digits indicates the GPDB version.
 */
#define PG_CONTROL_VERSION	8310500

/*
 * Body of CheckPoint XLOG records.  This is declared here because we keep
 * a copy of the latest one in pg_control for possible disaster recovery.
 */
typedef struct CheckPoint
{
	XLogRecPtr	redo;			/* next RecPtr available when we began to
								 * create CheckPoint (i.e. REDO start point) */
	XLogRecPtr	undo;			/* first record of oldest in-progress
								 * transaction when we started (i.e. UNDO end
								 * point) */
	TimeLineID	ThisTimeLineID; /* current TLI */
	uint32		nextXidEpoch;	/* higher-order bits of nextXid */
	TransactionId nextXid;		/* next free XID */
	Oid			nextOid;		/* next free OID */
	MultiXactId nextMulti;		/* next free MultiXactId */
	MultiXactOffset nextMultiOffset;	/* next free MultiXact offset */
	time_t		time;			/* time stamp of checkpoint */

	/* IN XLOG RECORD, MORE DATA FOLLOWS AT END OF STRUCT FOR DTM CHECKPOINT */

} CheckPoint;

/* XLOG info values for XLOG rmgr */
#define XLOG_CHECKPOINT_SHUTDOWN		0x00
#define XLOG_CHECKPOINT_ONLINE			0x10
#define XLOG_NEXTOID					0x30
#define XLOG_SWITCH						0x40
#define XLOG_BACKUP_END					0x50


/* System status indicator */
typedef enum DBState
{
	DB_STARTUP = 0,
	DB_SHUTDOWNED,
	DB_SHUTDOWNING,
	DB_IN_CRASH_RECOVERY,
	DB_IN_STANDBY_MODE,
	DB_IN_STANDBY_PROMOTED,
	DB_IN_STANDBY_NEW_TLI_SET,
	DB_IN_PRODUCTION
} DBState;

#define LOCALE_NAME_BUFLEN	128

/*
 * Contents of pg_control.
 *
 * NOTE: try to keep this under 512 bytes so that it will fit on one physical
 * sector of typical disk drives.  This reduces the odds of corruption due to
 * power failure midway through a write.  Currently it fits comfortably,
 * but we could probably reduce LOCALE_NAME_BUFLEN if things get tight.
 */

typedef struct ControlFileData
{
	/*
	 * Unique system identifier --- to ensure we match up xlog files with the
	 * installation that produced them.
	 */
	uint64		system_identifier;

	/*
	 * Version identifier information.	Keep these fields at the same offset,
	 * especially pg_control_version; they won't be real useful if they move
	 * around.	(For historical reasons they must be 8 bytes into the file
	 * rather than immediately at the front.)
	 *
	 * pg_control_version identifies the format of pg_control itself.
	 * catalog_version_no identifies the format of the system catalogs.
	 *
	 * There are additional version identifiers in individual files; for
	 * example, WAL logs contain per-page magic numbers that can serve as
	 * version cues for the WAL log.
	 */
	uint32		pg_control_version;		/* PG_CONTROL_VERSION */
	uint32		catalog_version_no;		/* see catversion.h */

	/*
	 * System status data
	 */
	DBState		state;			/* see enum above */
	time_t		time;			/* time stamp of last pg_control update */
	XLogRecPtr	checkPoint;		/* last check point record ptr */
	XLogRecPtr	prevCheckPoint; /* previous check point record ptr */

	CheckPoint	checkPointCopy; /* copy of last check point record */

	/*
	 * These values determine the minimum point we must recover up to
	 * before starting up:
	 *
	 * minRecoveryPoint use in GPDB is very limited. Currently, it is used
	 * to simply to store the location of end of backup in standby mode
	 * That guards against starting standby, aborting it, and restarting with
	 * an earlier stop location. We can't get promoted unless we've at-least
	 * replayed upto minRecoveryPoint
	 *
	 * backupStartPoint is the redo pointer of the backup start checkpoint, if
	 * we are recovering from an online backup and haven't reached the end of
	 * backup yet. It is reset to zero when the end of backup is reached, and
	 * we mustn't start up before that. A boolean would suffice otherwise, but
	 * we use the redo pointer as a cross-check when we see an end-of-backup
	 * record, to make sure the end-of-backup record corresponds the base
	 * backup we're recovering from.
	 *
	 * If backupEndRequired is true, we know for sure that we're restoring
	 * from a backup, and must see a backup-end record before we can safely
	 * start up. If it's false, but backupStartPoint is set, a backup_label
	 * file was found at startup but it may have been a leftover from a stray
	 * pg_start_backup() call, not accompanied by pg_stop_backup().
	 */
	XLogRecPtr	minRecoveryPoint;		/* must replay xlog to here */
	XLogRecPtr		backupStartPoint;
	bool		backupEndRequired;

	/*
	 * This data is used to check for hardware-architecture compatibility of
	 * the database and the backend executable.  We need not check endianness
	 * explicitly, since the pg_control version will surely look wrong to a
	 * machine of different endianness, but we do need to worry about MAXALIGN
	 * and floating-point format.  (Note: storage layout nominally also
	 * depends on SHORTALIGN and INTALIGN, but in practice these are the same
	 * on all architectures of interest.)
	 *
	 * Testing just one double value is not a very bulletproof test for
	 * floating-point compatibility, but it will catch most cases.
	 */
	uint32		maxAlign;		/* alignment requirement for tuples */
	double		floatFormat;	/* constant 1234567.0 */
#define FLOATFORMAT_VALUE	1234567.0

	/*
	 * This data is used to make sure that configuration of this database is
	 * compatible with the backend executable.
	 */
	uint32		blcksz;			/* data block size for this DB */
	uint32		relseg_size;	/* blocks per segment of large relation */

	uint32		xlog_blcksz;	/* block size within WAL files */
	uint32		xlog_seg_size;	/* size of each WAL segment */

	uint32		nameDataLen;	/* catalog name field width */
	uint32		indexMaxKeys;	/* max number of columns in an index */

	/* flag indicating internal format of timestamp, interval, time */
	uint32		enableIntTimes; /* int64 storage enabled? */

	/* active locales */
	uint32		localeBuflen;
	char		lc_collate[LOCALE_NAME_BUFLEN];
	char		lc_ctype[LOCALE_NAME_BUFLEN];

	/* CRC of all above ... MUST BE LAST! */
	pg_crc32c	crc;
} ControlFileData;

/*
 * Physical size of the pg_control file.  Note that this is considerably
 * bigger than the actually used size (ie, sizeof(ControlFileData)).
 * The idea is to keep the physical size constant independent of format
 * changes, so that ReadControlFile will deliver a suitable wrong-version
 * message instead of a read error if it's looking at an incompatible file.
 */
#define PG_CONTROL_SIZE		8192

#endif   /* PG_CONTROL_H */
