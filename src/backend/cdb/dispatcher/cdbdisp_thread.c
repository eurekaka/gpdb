
/*-------------------------------------------------------------------------
 *
 * cdbdisp_thread.c
 *	  Functions for multi-thread implementation of dispatching
 *	  commands to QExecutors.
 *
 * Copyright (c) 2005-2008, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include <pthread.h>
#include <limits.h>

#ifdef HAVE_POLL_H
#include <poll.h>
#endif
#ifdef HAVE_SYS_POLL_H
#include <sys/poll.h>
#endif

#include "storage/ipc.h"		/* For proc_exit_inprogress */
#include "tcop/tcopprot.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdisp_thread.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "miscadmin.h"
#include "utils/gp_atomic.h"

#ifndef _WIN32
#define mythread() ((unsigned long) pthread_self())
#else
#define mythread() ((unsigned long) pthread_self().p)
#endif

#define DISPATCH_WAIT_TIMEOUT_SEC 2

/*
 * Counter to indicate there are some dispatch threads running. This will
 * be incremented at the beginning of dispatch threads and decremented at
 * the end of them.
 */
static volatile int32 RunningThreadCount = 0;

static int	getMaxThreadsPerGang(void);

static bool
shouldStillDispatchCommand(DispatchCommandParms *pParms,
						   CdbDispatchResult *dispatchResult);

static void
CollectQEWriterTransactionInformation(SegmentDatabaseDescriptor *segdbDesc,
									  CdbDispatchResult *dispatchResult);

static void
dispatchCommand(CdbDispatchResult *dispatchResult,
				const char *query_text, int query_text_len);

/* returns true if command complete */
static bool processResults(CdbDispatchResult *dispatchResult);

static DispatchWaitMode
cdbdisp_signalQE(SegmentDatabaseDescriptor *segdbDesc,
				 DispatchWaitMode waitMode);

static void *thread_DispatchCommand(void *arg);
static void thread_DispatchOut(DispatchCommandParms *pParms);
static void thread_DispatchWait(DispatchCommandParms *pParms);

static void
handlepollerror(dispatchcommandparms *pparms, int db_count, int sock_errno);

static void
handlepolltimeout(dispatchcommandparms *pparms,
				  int db_count, int *timeoutcounter, bool usesampling);

static void
decrementrunningcount(void *arg);

void
cdbdisp_dispatchtogang_internal(struct cdbdispatcherstate *ds,
								struct gang *gp,
								int sliceindex,
								cdbdispatchdirectdesc *disp_direct)
{
	struct cdbdispatchresults *dispatchresults = ds->primaryresults;
	segmentdatabasedescriptor *segdbdesc;
	int	i,
		max_threads,
		segdbs_in_thread_pool = 0,
		newthreads = 0;
	int	gangsize = 0;
	segmentdatabasedescriptor *db_descriptors;
	dispatchcommandparms *pparms = null;

	gangsize = gp->size;
	assert(gangsize <= largestgangsize());
	db_descriptors = gp->db_descriptors;

	assert(gp_connections_per_thread > 0);
	assert(ds->dispatchthreads != null);
	/*
	 * if we attempt to reallocate, there is a race here: we
	 * know that we have threads running using the
	 * dispatchcommandparamsar! if we reallocate we
	 * potentially yank it out from under them! don't do
	 * it!
	 */
	max_threads = getmaxthreadspergang();
	if (ds->dispatchthreads->dispatchcommandparmsarsize <
		(ds->dispatchthreads->threadcount + max_threads))
	{
		elog(error,
			 "attempted to reallocate dispatchcommandparmsar while other threads still running size %d new threadcount %d",
			 ds->dispatchthreads->dispatchcommandparmsarsize,
			 ds->dispatchthreads->threadcount + max_threads);
	}

	/*
	 * create the thread parms structures based targetset parameter.
	 * this will add the segdbdesc pointers appropriate to the
	 * targetset into the thread parms structures, making sure that each thread
	 * handles gp_connections_per_thread segdbs.
	 */
	for (i = 0; i < gangsize; i++)
	{
		cdbdispatchresult *qeresult;

		segdbdesc = &db_descriptors[i];
		int	parmsindex = 0;

		assert(segdbdesc != null);

		if (disp_direct->directed_dispatch)
		{
			assert(disp_direct->count == 1);	/* currently we allow direct-to-one dispatch, only */

			if (disp_direct->content[0] != segdbdesc->segindex)
				continue;
		}

		/*
		 * initialize the qe's cdbdispatchresult object.
		 */
		qeresult = cdbdisp_makeresult(dispatchresults, segdbdesc, sliceindex);

		if (qeresult == null)
		{
			/*
			 * writer_gang could be null if this is an extended query.
			 */
			if (dispatchresults->writer_gang)
				dispatchresults->writer_gang->dispatcheractive = true;
			elog(fatal, "could not allocate resources for segworker communication");
		}

		parmsindex = segdbs_in_thread_pool / gp_connections_per_thread;
		pparms = ds->dispatchthreads->dispatchcommandparmsar + ds->dispatchthreads->threadcount + parmsindex;
		pparms->dispatchresultptrarray[pparms->db_count++] = qeresult;

		/*
		 * this cdbdispatchresult/segmentdatabasedescriptor pair will be
		 * dispatched and monitored by a thread to be started below. only that
		 * thread should touch them until the thread is finished with them and
		 * resets the stillrunning flag. caller must cdbcheckdispatchresult()
		 * to wait for completion.
		 */
		qeresult->stillrunning = true;

		segdbs_in_thread_pool++;
	}

	/*
	 * compute the thread count based on how many segdbs were added into the
	 * thread pool, knowing that each thread handles gp_connections_per_thread
	 * segdbs.
	 */
	assert(segdbs_in_thread_pool != 0);
	newthreads = 1 + (segdbs_in_thread_pool - 1) / gp_connections_per_thread;

	/*
	 * create the threads. (which also starts the dispatching).
	 */
	for (i = 0; i < newthreads; i++)
	{
		int pthread_err = 0;
		dispatchcommandparms *pparms = &(ds->dispatchthreads->dispatchcommandparmsar + ds->dispatchthreads->threadcount)[i];
		assert(pparms != null);

		pparms->thread_valid = true;

		pthread_err = gp_pthread_create(&pparms->thread, thread_dispatchcommand, pparms, "dispatchtogang");
		if (pthread_err != 0)
		{
			int j;

			pparms->thread_valid = false;

			/*
			 * error during thread create (this should be caused by
			 * resource constraints). if we leave the threads running,
			 * they'll immediately have some problems -- so we need to
			 * join them, and *then* we can issue our fatal error
			 */
			pparms->waitmode = dispatch_wait_cancel;

			for (j = 0; j < ds->dispatchthreads->threadcount + (i - 1); j++)
			{
				dispatchcommandparms *pparms;

				pparms = &ds->dispatchthreads->dispatchcommandparmsar[j];

				pparms->waitmode = dispatch_wait_cancel;
				pparms->thread_valid = false;
				pthread_join(pparms->thread, null);
			}

			ereport(fatal, (errcode(errcode_internal_error),
						    errmsg("could not create thread %d of %d", i + 1, newthreads),
							errdetail ("pthread_create() failed with err %d", pthread_err)));
		}
	}

	ds->dispatchthreads->threadcount += newthreads;
	elog(debug4, "dispatchtogang: total threads now %d",
		 ds->dispatchthreads->threadcount);
}

void
cdbcheckdispatchresult_internal(struct cdbdispatcherstate *ds,
								dispatchwaitmode waitmode)
{
	int	i;
	int	j;
	dispatchcommandparms *pparms;
	cdbdispatchresult *dispatchresult;

	assert(ds != null);

	/*
	 * no-op if no work was dispatched since the last time we were called.
	 */
	if (!ds->dispatchthreads || ds->dispatchthreads->threadcount == 0)
	{
		elog(debug5, "checkdispatchresult: no threads active");
		return;
	}

	/*
	 * wait for threads to finish.
	 */
	for (i = 0; i < ds->dispatchthreads->threadcount; i++)
	{
		pparms = &ds->dispatchthreads->dispatchcommandparmsar[i];
		assert(pparms != null);

		/*
		 * does caller want to stop short?
		 */
		switch (waitmode)
		{
			case dispatch_wait_cancel:
			case dispatch_wait_finish:
				pparms->waitmode = waitmode;
				break;
			default:
				break;
		}

		elog(debug4, "checkdispatchresult: joining to thread %d of %d", i + 1, ds->dispatchthreads->threadcount);

		if (pparms->thread_valid)
		{
			int pthread_err = 0;

			pthread_err = pthread_join(pparms->thread, null);
			if (pthread_err != 0)
				elog(fatal,
					"checkdispatchresult: pthread_join failed on thread %d (%lu) of %d (returned %d attempting to join to %lu)",
					i + 1,
#ifndef _win32
					(unsigned long) pparms->thread,
#else
					(unsigned long) pparms->thread.p,
#endif
					ds->dispatchthreads->threadcount, pthread_err,
					(unsigned long) mythread());
		}

		hold_interrupts();
		pparms->thread_valid = false;
		memset(&pparms->thread, 0, sizeof(pparms->thread));
		resume_interrupts();

		/*
		 * examine the cdbdispatchresult objects containing the results
		 * from this thread's qes.
		 */
		for (j = 0; j < pparms->db_count; j++)
		{
			dispatchresult = pparms->dispatchresultptrarray[j];

			if (dispatchresult == null)
			{
				elog(log, "checkdispatchresult: result object is null ? skipping.");
				continue;
			}

			if (dispatchresult->segdbdesc == null)
			{
				elog(log, "checkdispatchresult: result object segment descriptor is null ? skipping.");
				continue;
			}

			/*
			 * log the result
			 */
			if (debug2 >= log_min_messages)
				cdbdisp_debugdispatchresult(dispatchresult, debug2, debug3);

			/*
			 * zap our segmentdatabasedescriptor ptr because it may be
			 * invalidated by the call to ftshandlenetfailure() below.
			 * anything we need from there, we should get before this.
			 */
			dispatchresult->segdbdesc = null;
		}
	}

	/*
	 * reset thread state (will be destroyed later on in finishcommand)
	 */
	ds->dispatchthreads->threadcount = 0;

	/*
	 * it looks like everything went fine, make sure we don't miss a
	 * user cancellation?
	 *
	 * the waitmode argument is none when we are doing "normal work".
	 */
	if (waitmode == dispatch_wait_none || waitmode == dispatch_wait_finish)
		check_for_interrupts();
}

/*
 * synchronize threads to finish for this process to die. dispatching
 * threads need to acknowledge that we are dying, otherwise the main
 * thread will cleanup memory contexts which could cause process crash
 * while the threads are touching stale pointers. threads will check
 * proc_exit_inprogress and immediately stops once it's found to be true.
 */
void
cdbdisp_waitthreads(void)
{
	int	i,
		max_retry;
	long interval = 10 * 1000;	/* 10 msec */

	/*
	 * just in case to avoid to be stuck in the final stage of process
	 * lifecycle, insure by setting time limit. if it exceeds, it probably
	 * means some threads are stuck and not progressing, in which case
	 * we can go ahead and cleanup things anyway. the duration should be
	 * longer than the select timeout in thread_dispatchwait.
	 */
	max_retry = (dispatch_wait_timeout_sec + 10) * 1000000l / interval;

	/*
	 * this is supposed to be called after the flag is set.
	 */
	assert(proc_exit_inprogress);

	for (i = 0; i < max_retry; i++)
	{
		if (runningthreadcount == 0)
			break;
		pg_usleep(interval);
	}
}

/*
 * cdbdisp_makedispatchthreads:
 * allocates memory for a cdbdispatchcmdthreads structure and the memory
 * needed inside. do the initialization.
 * will be freed in function cdbdisp_destroydispatcherstate by deleting the
 * memory context.
 */
cdbdispatchcmdthreads *
cdbdisp_makedispatchthreads(int maxslices)
{
	int	maxthreadspergang = getmaxthreadspergang();
	int	maxthreads = maxthreadspergang * maxslices;

	int	maxconn = gp_connections_per_thread;
	int	size = 0;
	int	i = 0;
	cdbdispatchcmdthreads *dthreads = palloc0(sizeof(*dthreads));

	size = maxthreads * sizeof(dispatchcommandparms);
	dthreads->dispatchcommandparmsar = (dispatchcommandparms *) palloc0(size);
	dthreads->dispatchcommandparmsarsize = maxthreads;
	dthreads->threadcount = 0;

	for (i = 0; i < maxthreads; i++)
	{
		dispatchcommandparms *pparms = &dthreads->dispatchcommandparmsar[i];

		pparms->nfds = maxconn;
		memset(&pparms->thread, 0, sizeof(pthread_t));

		size = maxconn * sizeof(cdbdispatchresult *);
		pparms->dispatchresultptrarray = (cdbdispatchresult **) palloc0(size);

		size = sizeof(struct pollfd) * maxconn;
		pparms->fds = (struct pollfd *) palloc0(size);
	}

	return dthreads;
}

static void
thread_dispatchout(dispatchcommandparms *pparms)
{
	cdbdispatchresult *dispatchresult;
	int	i,
		db_count = pparms->db_count;

	/*
	 * the pparms contains an array of segmentdatabasedescriptors
	 * to send commands through to.
	 */
	for (i = 0; i < db_count; i++)
	{
		dispatchresult = pparms->dispatchresultptrarray[i];

		/*
		 * don't use elog, it's not thread-safe
		 */
		if (debug5 >= log_min_messages)
		{
			if (dispatchresult->segdbdesc->conn)
			{
				write_log
					("thread_dispatchcommand working on %d of %d commands. asyncstatus %d",
					 i + 1, db_count,
					 dispatchresult->segdbdesc->conn->asyncstatus);
			}
		}

		dispatchresult->hasdispatched = false;
		dispatchresult->sentsignal = dispatch_wait_none;
		dispatchresult->wascanceled = false;

		if (!shouldstilldispatchcommand(pparms, dispatchresult))
		{
			/*
			 * don't dispatch if cancellation pending or no connection.
			 */
			dispatchresult->stillrunning = false;
			if (pqisbusy(dispatchresult->segdbdesc->conn))
				write_log
					(" we thought we were done, because !shouldstilldispatchcommand(), but libpq says we are still busy");
			if (pqstatus(dispatchresult->segdbdesc->conn) == connection_bad)
				write_log
					(" we thought we were done, because !shouldstilldispatchcommand(), but libpq says the connection died?");
		}
		else
		{
			/*
			 * kick off the command over the libpq connection.
			 * * if unsuccessful, proceed anyway, and check for lost connection below.
			 */
			if (pqisbusy(dispatchresult->segdbdesc->conn))
			{
				write_log
					("trying to send to busy connection %s %d %d asyncstatus %d",
					 dispatchresult->segdbdesc->whoami, i, db_count,
					 dispatchresult->segdbdesc->conn->asyncstatus);
			}

			if (pqstatus(dispatchresult->segdbdesc->conn) == connection_bad)
			{
				char *msg;

				msg = pqerrormessage(dispatchresult->segdbdesc->conn);

				write_log
					("dispatcher noticed a problem before query transmit: %s (%s)",
					 msg ? msg : "unknown error",
					 dispatchresult->segdbdesc->whoami);

				/*
				 * save error info for later.
				 */
				cdbdisp_appendmessage(dispatchresult, log,
									  errcode_gp_interconnection_error,
									  "error before transmit from %s: %s",
									  dispatchresult->segdbdesc->whoami,
									  msg ? msg : "unknown error");

				pqfinish(dispatchresult->segdbdesc->conn);
				dispatchresult->segdbdesc->conn = null;
				dispatchresult->stillrunning = false;

				continue;
			}

			dispatchcommand(dispatchresult, pparms->query_text,
							pparms->query_text_len);
		}
	}
}

static void
thread_dispatchwait(dispatchcommandparms *pparms)
{
	segmentdatabasedescriptor *segdbdesc;
	cdbdispatchresult *dispatchresult;
	int	i,
		db_count = pparms->db_count;
	int	timeoutcounter = 0;

	/*
	 * ok, we are finished submitting the command to the segdbs.
	 * now, we have to wait for them to finish.
	 */
	for (;;)
	{
		int	sock;
		int	n;
		int	nfds = 0;
		int	cur_fds_num = 0;

		/*
		 * which qes are still running and could send results to us?
		 */
		for (i = 0; i < db_count; i++)
		{
			dispatchresult = pparms->dispatchresultptrarray[i];
			segdbdesc = dispatchresult->segdbdesc;

			/*
			 * already finished with this qe?
			 */
			if (!dispatchresult->stillrunning)
				continue;

			/*
			 * add socket to fd_set if still connected.
			 */
			sock = pqsocket(segdbdesc->conn);
			if (sock >= 0 && pqstatus(segdbdesc->conn) != connection_bad)
			{
				pparms->fds[nfds].fd = sock;
				pparms->fds[nfds].events = pollin;
				nfds++;
				assert(nfds <= pparms->nfds);
			}

			/*
			 * lost the connection.
			 */
			else
			{
				char *msg = pqerrormessage(segdbdesc->conn);

				/*
				 * save error info for later.
				 */
				cdbdisp_appendmessage(dispatchresult, debug1,
									  errcode_gp_interconnection_error,
									  "lost connection to %s. %s",
									  segdbdesc->whoami, msg ? msg : "");

				/*
				 * free the pgconn object.
				 */
				pqfinish(segdbdesc->conn);
				segdbdesc->conn = null;
				dispatchresult->stillrunning = false;
			}
		}

		/*
		 * break out when no qes still running.
		 */
		if (nfds <= 0)
			break;

		/*
		 * bail-out if we are dying. we should not do much of cleanup
		 * as the main thread is waiting on this thread to finish.	once
		 * qd dies, qe will recognize it shortly anyway.
		 */
		if (proc_exit_inprogress)
			break;

		/*
		 * wait for results from qes. block here until input is available.
		 */
		n = poll(pparms->fds, nfds, dispatch_wait_timeout_sec * 1000);

		if (n < 0)
		{
			int	sock_errno = sock_errno;

			if (sock_errno == eintr)
				continue;

			handlepollerror(pparms, db_count, sock_errno);
			continue;
		}

		if (n == 0)
		{
			handlepolltimeout(pparms, db_count, &timeoutcounter, true);
			continue;
		}

		cur_fds_num = 0;
		/*
		 * we have data waiting on one or more of the connections.
		 */
		for (i = 0; i < db_count; i++)
		{
			bool finished;

			dispatchresult = pparms->dispatchresultptrarray[i];
			segdbdesc = dispatchresult->segdbdesc;

			/*
			 * skip if already finished or didn't dispatch.
			 */
			if (!dispatchresult->stillrunning)
				continue;

			if (debug4 >= log_min_messages)
				write_log("looking for results from %d of %d", i + 1,
						  db_count);

			/*
			 * skip this connection if it has no input available.
			 */
			sock = pqsocket(segdbdesc->conn);
			if (sock >= 0)
			{
				/*
				 * the fds array is shorter than conn array, so the following
				 * match method will use this assumtion.
				 */
				assert(sock == pparms->fds[cur_fds_num].fd);
			}
			if (sock >= 0 && (sock == pparms->fds[cur_fds_num].fd))
			{
				cur_fds_num++;
				if (!(pparms->fds[cur_fds_num - 1].revents & pollin))
					continue;
			}

			if (debug4 >= log_min_messages)
				write_log("pqsocket says there are results from %d", i + 1);
			/*
			 * receive and process results from this qe.
			 */
			finished = processresults(dispatchresult);

			/*
			 * are we through with this qe now?
			 */
			if (finished)
			{
				if (debug4 >= log_min_messages)
					write_log
						("processresults says we are finished with %d: %s",
						 i + 1, segdbdesc->whoami);
				dispatchresult->stillrunning = false;
				if (debug1 >= log_min_messages)
				{
					char msec_str[32];

					switch (check_log_duration(msec_str, false))
					{
						case 1:
						case 2:
							write_log
								("duration to dispatch result received from thread %d (seg %d): %s ms",
								 i + 1, dispatchresult->segdbdesc->segindex,
								 msec_str);
							break;
					}
				}
				if (pqisbusy(dispatchresult->segdbdesc->conn))
					write_log
						("we thought we were done, because finished==true, but libpq says we are still busy");

			}
			else if (debug4 >= log_min_messages)
				write_log("processresults says we have more to do with %d: %s",
						  i + 1, segdbdesc->whoami);
		}
	}
}

/*
 * cleanup routine for the dispatching thread.	this will indicate the thread
 * is not running any longer.
 */
static void
decrementrunningcount(void *arg)
{
	pg_atomic_sub_fetch_u32((pg_atomic_uint32 *) &runningthreadcount, 1);
}

/*
 * thread_dispatchcommand is the thread proc used to dispatch the command to one or more of the qexecs.
 *
 * note: this function must not contain elog or ereport statements. (or most any other backend code)
 *		 elog is not thread-safe. developers should instead use something like:
 *
 *	if (debug3 >= log_min_messages)
 *			write_log("my brilliant log statement here.");
 *
 * note: in threads, we cannot use palloc, because it's not thread safe.
 */
static void *
thread_dispatchcommand(void *arg)
{
	dispatchcommandparms *pparms = (dispatchcommandparms *) arg;

	gp_set_thread_sigmasks();

	/*
	 * mark that we are runnig a new thread. the main thread will check
	 * it to see if there is still alive one. let's do this after we block
	 * signals so that nobody will intervent and mess up the value.
	 * (should we actually block signals before spawning a thread, as much
	 * like we do in fork??)
	 */
	pg_atomic_add_fetch_u32((pg_atomic_uint32 *) &runningthreadcount, 1);

	/*
	 * we need to make sure the value will be decremented once the thread
	 * finishes. currently there is not such case but potentially we could
	 * have pthread_exit or thread cancellation in the middle of code, in
	 * which case we would miss to decrement value if we tried to do this
	 * without the cleanup callback facility.
	 */
	pthread_cleanup_push(decrementrunningcount, null);
	{
		thread_dispatchout(pparms);
		thread_dispatchwait(pparms);
	}
	pthread_cleanup_pop(1);

	return (null);
}

/*
 * helper function to thread_dispatchcommand that actually kicks off the
 * command on the libpq connection.
 *
 * note: since this is called via a thread, the same rules apply as to
 *		 thread_dispatchcommand absolutely no elog'ing.
 */
static void
dispatchcommand(cdbdispatchresult *dispatchresult,
				const char *query_text, int query_text_len)
{
	segmentdatabasedescriptor *segdbdesc = dispatchresult->segdbdesc;
	pgconn *conn = segdbdesc->conn;
	timestamptz beforesend = 0;
	long secs;
	int	usecs;

	if (debug1 >= log_min_messages)
		beforesend = getcurrenttimestamp();

	/*
	 * submit the command asynchronously.
	 */
	if (pqsendgpquery_shared(conn, (char *) query_text, query_text_len) == 0)
	{
		char *msg = pqerrormessage(segdbdesc->conn);

		if (debug3 >= log_min_messages)
			write_log("pqsendmppquery_shared error %s %s",
					  segdbdesc->whoami, msg ? msg : "");

		/*
		 * note the error.
		 */
		cdbdisp_appendmessage(dispatchresult, log,
							  errcode_gp_interconnection_error,
							  "command could not be sent to segment db %s; %s",
							  segdbdesc->whoami, msg ? msg : "");
		pqfinish(conn);
		segdbdesc->conn = null;
		dispatchresult->stillrunning = false;
	}

	if (debug1 >= log_min_messages)
	{
		timestampdifference(beforesend, getcurrenttimestamp(), &secs, &usecs);

		if (secs != 0 || usecs > 1000)	/* time > 1ms? */
			write_log("time for pqsendgpquery_shared %ld.%06d", secs, usecs);
	}

	dispatchresult->hasdispatched = true;
	/*
	 * we'll keep monitoring this qe -- whether or not the command
	 * was dispatched -- in order to check for a lost connection
	 * or any other errors that libpq might have in store for us.
	 */
}

/*
 * helper function to thread_dispatchcommand that handles errors that occur
 * during the poll() call.
 *
 * note: since this is called via a thread, the same rules apply as to
 *		 thread_dispatchcommand absolutely no elog'ing.
 *		 the cleanup of the connections will be performed by handlepolltimeout().
 */
static void
handlepollerror(dispatchcommandparms *pparms, int db_count, int sock_errno)
{
	int	i;
	int	forcetimeoutcount;

	if (log >= log_min_messages)
	{
		/*
		 * don't use elog, it's not thread-safe
		 */
		write_log("handlepollerror poll() failed; errno=%d", sock_errno);
	}

	/*
	 * based on the select man page, we could get here with
	 * errno == ebadf (bad descriptor), einval (highest descriptor negative or negative timeout)
	 * or enomem (out of memory).
	 * this is most likely a programming error or a bad system failure, but we'll try to
	 * clean up a bit anyhow.
	 *
	 * we *can* get here as a result of some hardware issues. the timeout code
	 * knows how to clean up if we've lost contact with one of our peers.
	 *
	 * we should check a connection's integrity before calling pqisbusy().
	 */
	for (i = 0; i < db_count; i++)
	{
		cdbdispatchresult *dispatchresult = pparms->dispatchresultptrarray[i];

		/*
		 * skip if already finished or didn't dispatch.
		 */
		if (!dispatchresult->stillrunning)
			continue;

		/*
		 * we're done with this qe, sadly.
		 */
		if (pqstatus(dispatchresult->segdbdesc->conn) == connection_bad)
		{
			char *msg;

			msg = pqerrormessage(dispatchresult->segdbdesc->conn);
			if (msg)
				write_log("dispatcher encountered connection error on %s: %s",
						  dispatchresult->segdbdesc->whoami, msg);

			write_log
				("dispatcher noticed bad connection in handlepollerror()");

			/*
			 * save error info for later.
			 */
			cdbdisp_appendmessage(dispatchresult, log,
								  errcode_gp_interconnection_error,
								  "error after dispatch from %s: %s",
								  dispatchresult->segdbdesc->whoami,
								  msg ? msg : "unknown error");

			pqfinish(dispatchresult->segdbdesc->conn);
			dispatchresult->segdbdesc->conn = null;
			dispatchresult->stillrunning = false;
		}
	}

	forcetimeoutcount = 60;		/* anything bigger than 30 */
	handlepolltimeout(pparms, db_count, &forcetimeoutcount, false);

	return;

	/*
	 * no point in trying to cancel the other qes with select() broken.
	 */
}

/*
 * helper function to thread_dispatchcommand that handles timeouts that occur
 * during the poll() call.
 *
 * note: since this is called via a thread, the same rules apply as to
 *		 thread_dispatchcommand absolutely no elog'ing.
 */
static void
handlepolltimeout(dispatchcommandparms *pparms,
				  int db_count, int *timeoutcounter, bool usesampling)
{
	cdbdispatchresult *dispatchresult;
	cdbdispatchresults *meleeresults;
	segmentdatabasedescriptor *segdbdesc;
	int i;

	/*
	 * are there any qes that should be canceled?
	 */
	for (i = 0; i < db_count; i++)
	{
		dispatchwaitmode waitmode;

		dispatchresult = pparms->dispatchresultptrarray[i];
		if (dispatchresult == null)
			continue;
		segdbdesc = dispatchresult->segdbdesc;
		meleeresults = dispatchresult->meleeresults;

		/*
		 * already finished with this qe?
		 */
		if (!dispatchresult->stillrunning)
			continue;

		waitmode = dispatch_wait_none;

		/*
		 * send query finish to this qe if qd is already done.
		 */
		if (pparms->waitmode == dispatch_wait_finish)
			waitmode = dispatch_wait_finish;

		/*
		 * however, escalate it to cancel if:
		 *	 - user interrupt has occurred,
		 *	 - or i'm told to send cancel,
		 *	 - or an error has been reported by another qe,
		 *	 - in case the caller wants cancelonerror and it was not canceled
		 */
		if ((interruptpending ||
			 pparms->waitmode == dispatch_wait_cancel ||
			 meleeresults->errcode) &&
			(meleeresults->cancelonerror && !dispatchresult->wascanceled))
			waitmode = dispatch_wait_cancel;

		/*
		 * finally, don't send the signal if
		 *	 - no action needed (none)
		 *	 - the signal was already sent
		 *	 - connection is dead
		 */
		if (waitmode != dispatch_wait_none &&
			waitmode != dispatchresult->sentsignal &&
			pqstatus(segdbdesc->conn) != connection_bad)
		{
			dispatchresult->sentsignal = cdbdisp_signalqe(segdbdesc, waitmode);
		}
	}

	/*
	 * check the connection still valid, set 1 min time interval
	 * this may affect performance, should turn it off if required.
	 */
	if ((*timeoutcounter)++ > 30)
	{
		*timeoutcounter = 0;

		for (i = 0; i < db_count; i++)
		{
			dispatchresult = pparms->dispatchresultptrarray[i];
			segdbdesc = dispatchresult->segdbdesc;

			if (debug5 >= log_min_messages)
				write_log("checking status %d of %d %s stillrunning %d",
						  i + 1, db_count, segdbdesc->whoami,
						  dispatchresult->stillrunning);

			/*
			 * skip if already finished or didn't dispatch.
			 */
			if (!dispatchresult->stillrunning)
				continue;

			/*
			 * if we hit the timeout, and the query has already been
			 * cancelled we'll try to re-cancel here.
			 *
			 * xxx we may not need this anymore. it might be harmful
			 * rather than helpful, as it creates another connection.
			 */
			if (dispatchresult->sentsignal == dispatch_wait_cancel &&
				pqstatus(segdbdesc->conn) != connection_bad)
			{
				dispatchresult->sentsignal =
					cdbdisp_signalqe(segdbdesc, dispatch_wait_cancel);
			}

			/*
			 * skip the entry db.
			 */
			if (segdbdesc->segindex < 0)
				continue;

			if (debug5 >= log_min_messages)
				write_log("testing connection %d of %d %s stillrunning %d",
						  i + 1, db_count, segdbdesc->whoami,
						  dispatchresult->stillrunning);

			if (!ftstestconnection(segdbdesc->segment_database_info, false))
			{
				/*
				 * note the error.
				 */
				cdbdisp_appendmessage(dispatchresult, debug1,
									  errcode_gp_interconnection_error,
									  "lost connection to one or more segments - fault detector checking for segment failures. (%s)",
									  segdbdesc->whoami);

				/*
				 * not a good idea to store into the pgconn object. instead,
				 * just close it.
				 */
				pqfinish(segdbdesc->conn);
				segdbdesc->conn = null;

				/*
				 * this connection is hosed.
				 */
				dispatchresult->stillrunning = false;
			}
		}
	}

}

static int
getmaxthreadspergang(void)
{
	return 1 + (largestgangsize() - 1) / gp_connections_per_thread;
}

/*
 * helper function to thread_dispatchcommand that decides if we should dispatch
 * to this segment database.
 *
 * (1) don't dispatch if there is already a query cancel notice pending.
 * (2) make sure our libpq connection is still good.
 *
 * note: since this is called via a thread, the same rules apply as to
 *		 thread_dispatchcommand absolutely no elog'ing.
 */
static bool
shouldstilldispatchcommand(dispatchcommandparms *pparms,
						   cdbdispatchresult *dispatchresult)
{
	segmentdatabasedescriptor *segdbdesc = dispatchresult->segdbdesc;
	cdbdispatchresults *gangresults = dispatchresult->meleeresults;

	/*
	 * don't dispatch to a qe that is not connected. note, that pqstatus() correctly
	 * handles the case where segdbdesc->conn is null, and we *definitely* want to
	 * produce an error for that case.
	 */
	if (pqstatus(segdbdesc->conn) == connection_bad)
	{
		char *msg = pqerrormessage(segdbdesc->conn);

		/*
		 * save error info for later.
		 */
		cdbdisp_appendmessage(dispatchresult, log,
							  errcode_gp_interconnection_error,
							  "lost connection to %s. %s",
							  segdbdesc->whoami, msg ? msg : "");

		if (debug4 >= log_min_messages)
		{
			/*
			 * don't use elog, it's not thread-safe
			 */
			write_log("lost connection: %s", segdbdesc->whoami);
		}

		/*
		 * free the pgconn object at once whenever we notice it's gone bad.
		 */
		pqfinish(segdbdesc->conn);
		segdbdesc->conn = null;
		dispatchresult->stillrunning = false;

		return false;
	}

	/*
	 * don't submit if already encountered an error. the error has already
	 * been noted, so just keep quiet.
	 */
	if (pparms->waitmode == dispatch_wait_cancel || gangresults->errcode)
	{
		if (gangresults->cancelonerror)
		{
			dispatchresult->wascanceled = true;

			if (debug_cancel_print || debug4 >= log_min_messages)
			{
				/*
				 * don't use elog, it's not thread-safe
				 */
				write_log("error cleanup in progress; command not sent to %s",
						  segdbdesc->whoami);
			}
			return false;
		}
	}

	/*
	 * don't submit if client told us to cancel. the cancellation request has
	 * already been noted, so hush.
	 */
	if (interruptpending && gangresults->cancelonerror)
	{
		dispatchresult->wascanceled = true;
		if (debug_cancel_print || debug4 >= log_min_messages)
			write_log("cancellation request pending; command not sent to %s",
					  segdbdesc->whoami);
		return false;
	}

	return true;
}

static bool
processresults(cdbdispatchresult *dispatchresult)
{
	segmentdatabasedescriptor *segdbdesc = dispatchresult->segdbdesc;
	char *msg;
	int	rc;

	/*
	 * pqisbusy() has side-effects
	 */
	if (debug5 >= log_min_messages)
	{
		write_log("processresults. isbusy = %d", pqisbusy(segdbdesc->conn));

		if (pqstatus(segdbdesc->conn) == connection_bad)
			goto connection_error;
	}

	/*
	 * receive input from qe.
	 */
	rc = pqconsumeinput(segdbdesc->conn);

	/*
	 * if pqconsumeinput fails, we're hosed.
	 */
	if (rc == 0)
	{							/* handle pqconsumeinput error */
		goto connection_error;
	}

	/*
	 * pqisbusy() has side-effects
	 */
	if (debug4 >= log_min_messages && pqisbusy(segdbdesc->conn))
		write_log("pqisbusy");

	/*
	 * if we have received one or more complete messages, process them.
	 */
	while (!pqisbusy(segdbdesc->conn))
	{
		/* loop to call pqgetresult; won't block */
		pgresult *pres;
		execstatustype resultstatus;
		int	resultindex;

		/*
		 * pqisbusy() does some error handling, which can
		 * cause the connection to die -- we can't just continue on as
		 * if the connection is happy without checking first.
		 *
		 * for example, cdbdisp_numpgresult() will return a completely
		 * bogus value!
		 */
		if (pqstatus(segdbdesc->conn) == connection_bad
			|| segdbdesc->conn->sock == -1)
		{
			goto connection_error;
		}

		resultindex = cdbdisp_numpgresult(dispatchresult);

		if (debug4 >= log_min_messages)
			write_log("pqgetresult");
		/*
		 * get one message.
		 */
		pres = pqgetresult(segdbdesc->conn);

		collectqewritertransactioninformation(segdbdesc, dispatchresult);

		/*
		 * command is complete when pggetresult() returns null. it is critical
		 * that for any connection that had an asynchronous command sent thru
		 * it, we call pqgetresult until it returns null. otherwise, the next
		 * time a command is sent to that connection, it will return an error
		 * that there's a command pending.
		 */
		if (!pres)
		{
			if (debug4 >= log_min_messages)
			{
				/*
				 * don't use elog, it's not thread-safe
				 */
				write_log("%s -> idle", segdbdesc->whoami);
			}
			/* this is normal end of command */
			return true;
		} /* end of results */


		/*
		 * attach the pgresult object to the cdbdispatchresult object.
		 */
		cdbdisp_appendresult(dispatchresult, pres);

		/*
		 * did a command complete successfully?
		 */
		resultstatus = pqresultstatus(pres);
		if (resultstatus == pgres_command_ok ||
			resultstatus == pgres_tuples_ok ||
			resultstatus == pgres_copy_in || resultstatus == pgres_copy_out)
		{

			/*
			 * save the index of the last successful pgresult. can be given to
			 * cdbdisp_getpgresult() to get tuple count, etc.
			 */
			dispatchresult->okindex = resultindex;

			if (debug3 >= log_min_messages)
			{
				/*
				 * don't use elog, it's not thread-safe
				 */
				char *cmdstatus = pqcmdstatus(pres);

				write_log("%s -> ok %s",
						  segdbdesc->whoami,
						  cmdstatus ? cmdstatus : "(no cmdstatus)");
			}

			/*
			 * sreh - get number of rows rejected from qe if any
			 */
			if (pres->numrejected > 0)
				dispatchresult->numrowsrejected += pres->numrejected;

			if (resultstatus == pgres_copy_in ||
				resultstatus == pgres_copy_out)
				return true;
		}

		/*
		 * note qe error. cancel the whole statement if requested.
		 */
		else
		{
			/* qe reported an error */
			char *sqlstate = pqresulterrorfield(pres, pg_diag_sqlstate);
			int errcode = 0;

			msg = pqresulterrormessage(pres);

			if (debug2 >= log_min_messages)
			{
				/*
				 * don't use elog, it's not thread-safe
				 */
				write_log("%s -> %s %s %s",
						  segdbdesc->whoami,
						  pqresstatus(resultstatus),
						  sqlstate ? sqlstate : "(no sqlstate)",
						  msg ? msg : "");
			}

			/*
			 * convert sqlstate to an error code (errcode_xxx). use a generic
			 * nonzero error code if no sqlstate.
			 */
			if (sqlstate && strlen(sqlstate) == 5)
				errcode = sqlstate_to_errcode(sqlstate);

			/*
			 * save first error code and the index of its pgresult buffer
			 * entry.
			 */
			cdbdisp_seterrcode(errcode, resultindex, dispatchresult);
		}
	}

	return false; /* we must keep on monitoring this socket */

connection_error:
	msg = pqerrormessage(segdbdesc->conn);

	if (msg)
		write_log("dispatcher encountered connection error on %s: %s",
				  segdbdesc->whoami, msg);

	/*
	 * save error info for later.
	 */
	cdbdisp_appendmessage(dispatchresult, log,
						  errcode_gp_interconnection_error,
						  "error on receive from %s: %s",
						  segdbdesc->whoami, msg ? msg : "unknown error");

	/*
	 * can't recover, so drop the connection.
	 */
	pqfinish(segdbdesc->conn);
	segdbdesc->conn = null;
	dispatchresult->stillrunning = false;

	return true; /* connection is gone! */
}

static void
collectqewritertransactioninformation(segmentdatabasedescriptor *segdbdesc,
									  cdbdispatchresult *dispatchresult)
{
	pgconn *conn = segdbdesc->conn;

	if (conn && conn->qewriter_haveinfo)
	{
		dispatchresult->qeisprimary = true;
		dispatchresult->qewriter_haveinfo = true;
		dispatchresult->qewriter_distributedtransactionid = conn->qewriter_distributedtransactionid;
		dispatchresult->qewriter_commandid = conn->qewriter_commandid;
		if (conn && conn->qewriter_dirty)
		{
			dispatchresult->qewriter_dirty = true;
		}
	}
}

/*
 * Send cancel/finish signal to still-running QE through libpq.
 * waitMode is either CANCEL or FINISH. Returns true if we successfully
 * sent a signal (not necessarily received by the target process).
 */
static DispatchWaitMode
cdbdisp_signalQE(SegmentDatabaseDescriptor *segdbDesc,
				 DispatchWaitMode waitMode)
{
	char errbuf[256];
	PGcancel *cn = PQgetCancel(segdbDesc->conn);
	int	ret = 0;

	if (cn == NULL)
		return DISPATCH_WAIT_NONE;

	/*
	 * PQcancel uses some strcpy/strcat functions; let's
	 * clear this for safety.
	 */
	MemSet(errbuf, 0, sizeof(errbuf));

	if (Debug_cancel_print || DEBUG4 >= log_min_messages)
		write_log("Calling PQcancel for %s", segdbDesc->whoami);

	/*
	 * Send query-finish, unless the client really wants to cancel the
	 * query. This could happen if cancel comes after we sent finish.
	 */
	if (waitMode == DISPATCH_WAIT_CANCEL)
		ret = PQcancel(cn, errbuf, 256);
	else if (waitMode == DISPATCH_WAIT_FINISH)
		ret = PQrequestFinish(cn, errbuf, 256);
	else
		write_log("unknown waitMode: %d", waitMode);

	if (ret == 0 && (Debug_cancel_print || LOG >= log_min_messages))
		write_log("Unable to cancel: %s", errbuf);

	PQfreeCancel(cn);

	return (ret != 0 ? waitMode : DISPATCH_WAIT_NONE);
}
