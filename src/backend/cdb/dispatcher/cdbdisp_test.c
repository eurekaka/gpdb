#include "postgres.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbconn.h"

extern void ShowUsage(const char *title);
extern bool FtsTestConnection(CdbComponentDatabaseInfo *failedDBInfo, bool fullScan);
extern int check_log_duration(char *msec_str, bool was_logged);
extern void cdbdisp_fillParms(DispatchCommandParms *pParms);

/* guc */
int			log_min_messages = WARNING;
bool		log_dispatch_stats = false;
bool		gp_use_dispatch_agent = false;
int			gp_connections_per_thread = 64;
bool		Debug_cancel_print = false;
volatile bool InterruptPending = false;
volatile int32 InterruptHoldoffCount = 0;


/* global variables */
bool		proc_exit_inprogress = false;
pthread_t main_tid = (pthread_t)0;
GpRoleValue Gp_role = GP_ROLE_DISPATCH;
ErrorContextCallback *error_context_stack = NULL;
sigjmp_buf *PG_exception_stack = NULL;



/* assert */
bool		assert_enabled = true;
int
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int lineNumber)
{
	fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			errorType, conditionName,
			fileName, lineNumber);
	abort();
	return 0;
}


/* try-catch */
void
pg_re_throw(void)
{
	abort();
}


/* log */
int
errcode_for_file_access(void)
{
	return 0;
}
bool errstart(int elevel, const char *filename, int lineno,
		 const char *funcname, const char *domain)
{
	return (elevel >= ERROR);
}

void
errfinish(int dummy,...)
{
	exit(1);
}

void
elog_start(const char *filename, int lineno, const char *funcname)
{
}

void
elog_finish(int elevel, const char *fmt,...)
{
	fprintf(stderr, "ERROR");
    va_list args;
    va_start(args, fmt);
	fprintf(stderr, fmt, args);
	va_end(args);
	fprintf(stderr, "\n");
	if (elevel >= ERROR)
		exit(1);
}


int
elog_geterrcode(void)
{
	return 0;
}

int
errOmitLocation(bool omitLocation)
{
	return 0;					/* return value does not matter */
}

char*
elog_message(void)
{
	return NULL;
}

int
errcode(int sqlerrcode)
{
	return 0;					/* return value does not matter */
}

int
errmsg(const char *fmt,...)
{
	fprintf(stderr, "ERROR: %s\n", fmt);
	return 0;					/* return value does not matter */
}

int
errmsg_internal(const char *fmt,...)
{
	fprintf(stderr, "ERROR: %s\n", fmt);
	return 0;					/* return value does not matter */
}

int
errdetail(const char *fmt,...)
{
	fprintf(stderr, "DETAIL: %s\n", fmt);
	return 0;					/* return value does not matter */
}

int
errdetail_log(const char *fmt,...)
{
	fprintf(stderr, "DETAIL: %s\n", fmt);
	return 0;					/* return value does not matter */
}

int
errhint(const char *fmt,...)
{
	fprintf(stderr, "HINT: %s\n", fmt);
	return 0;					/* return value does not matter */
}

void
write_stderr(const char *fmt,...)
{
	fprintf(stderr, "ERROR: %s\n", fmt);
}

void
write_log(const char *fmt,...)
{
	fprintf(stderr, "LOG: %s\n", fmt);
}

/* memory context */
static MemoryContextData memcontext;
MemoryContext TopMemoryContext = &memcontext;
MemoryContext CurrentMemoryContext = &memcontext;

void *
MemoryContextAllocZeroImpl(MemoryContext context, Size size, const char *sfile, const char *sfunc, int sline)
{
	void           *ptr = malloc(size);
	memset(ptr, 0, size);
	return ptr;
}

void
MemoryContextFreeImpl(void *pointer, const char *sfile, const char *sfunc, int sline)
{
	free(pointer);
}

void           *
MemoryContextAllocImpl(MemoryContext context, Size size, const char *sfile, const char *sfunc, int sline)
{
	return malloc(size);
}

char *
MemoryContextStrdup(MemoryContext context, const char *string)
{
	char       *nstr;
	Size        len = strlen(string) + 1;

	nstr = (char *) malloc (len);

	memcpy(nstr, string, len);

	return nstr;
}

void *
MemoryContextReallocImpl(void *pointer, Size size, const char *sfile, const char *sfunc, int sline)
{
	return realloc( pointer, size );
}

MemoryContext
AllocSetContextCreate(MemoryContext parent,
					  const char *name,
					  Size minContextSize,
					  Size initBlockSize,
					  Size maxBlockSize)
{
	return TopMemoryContext;
}

/* fault injection */
SimExESSubClass simex_check(const char *file, int32 line)
{
	return 0;
}

/* thread */
int
gp_pthread_create(pthread_t * thread,
				  void *(*start_routine) (void *),
				  void *arg, const char *caller)
{
	int			pthread_err = 0;
	pthread_attr_t t_atts;

	/*
	 * Call some init function. Before any thread is created, we need to init
	 * some static stuff. The main purpose is to guarantee the non-thread safe
	 * stuff are called in main thread, before any child thread get running.
	 * Note these staic data structure should be read only after init.	Thread
	 * creation is a barrier, so there is no need to get lock before we use
	 * these data structures.
	 *
	 * So far, we know we need to do this for getpwuid_r (See MPP-1971, glibc
	 * getpwuid_r is not thread safe).
	 */
#ifndef WIN32
	get_gp_passwdptr();
#endif

	/*
	 * save ourselves some memory: the defaults for thread stack size are
	 * large (1M+)
	 */
	pthread_err = pthread_attr_init(&t_atts);
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_init failed.  Error %d", caller, pthread_err);
		return pthread_err;
	}

#ifdef pg_on_solaris
	/* Solaris doesn't have PTHREAD_STACK_MIN ? */
	pthread_err = pthread_attr_setstacksize(&t_atts, (256 * 1024));
#else
#define PTHREAD_STACK_MIN 		8192
	pthread_err = pthread_attr_setstacksize(&t_atts, Max(PTHREAD_STACK_MIN, (256 * 1024)));
#endif
	if (pthread_err != 0)
	{
		elog(LOG, "%s: pthread_attr_setstacksize failed.  Error %d", caller, pthread_err);
		pthread_attr_destroy(&t_atts);
		return pthread_err;
	}

	pthread_err = pthread_create(thread, &t_atts, start_routine, arg);

	pthread_attr_destroy(&t_atts);

	return pthread_err;
}

void
gp_set_thread_sigmasks(void)
{
#ifndef WIN32
	sigset_t sigs;

	if (pthread_equal(main_tid, pthread_self()))
	{
		elog(LOG, "thread_mask called from main thread!");
		return;
	}

	sigemptyset(&sigs);

	/* make our thread ignore these signals (which should allow that
	 * they be delivered to the main thread) */
	sigaddset(&sigs, SIGHUP);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);
	sigaddset(&sigs, SIGALRM);
	sigaddset(&sigs, SIGUSR1);
	sigaddset(&sigs, SIGUSR2);

	pthread_sigmask(SIG_BLOCK, &sigs, NULL);
#endif

	return;
}

void cdbdisp_fillParms(DispatchCommandParms *pParms)
{
        pParms->sessUserId = 0;
        pParms->outerUserId = 0;
        pParms->currUserId = 0;
        pParms->sessUserId_is_super = false;
        pParms->outerUserId_is_super = false;
        pParms->cmdID = 0;
}

int getgpsegmentCount(void)
{
	return 100;
}

int largestGangsize(void)
{
	return 20;
}

bool FtsTestConnection(CdbComponentDatabaseInfo *failedDBInfo, bool fullScan)
{
	return true;
}

int check_log_duration(char *msec_str, bool was_logged)
{
	return 0;
}

void ShowUsage(const char *title)
{
}

void ReportSrehResults(CdbSreh *cdbsreh, int total_rejected)
{
}

bool
cdbconn_setSliceIndex(SegmentDatabaseDescriptor    *segdbDesc,
                      int                           sliceIndex)
{
    return true;
}

int GetDatabaseEncoding(void)
{
	return PG_UTF8;
}

int pg_char_to_encoding(const char *name)
{
	return PG_UTF8;
}

void
pg_usleep(long microsec)
{
	if (microsec > 0)
	{
		struct timeval delay;

		delay.tv_sec = microsec / 1000000L;
		delay.tv_usec = microsec % 1000000L;
		(void) select(0, NULL, NULL, NULL, &delay);
	}
}

#define DEF_ENC2NAME(name, codepage) { #name, PG_##name }
pg_enc2name pg_enc2name_tbl[] =
{
	DEF_ENC2NAME(UTF8, 65001),
};




/* test */
static void testBuildDispatchString(DispatchCommandParms *pParms);
static void testDispatchCommand(struct CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms);
static void testDispatchDestroy(DispatchCommandParms *pParms);
static void testDispatchInit(DispatchCommandParms *pParms, void *inputParms);

DispatchType TestDispatchType = {
		GP_DISPATCH_COMMAND_TYPE_DTX_PROTOCOL,
		testBuildDispatchString,
		testDispatchCommand,
		testDispatchInit,
		testDispatchDestroy
};

static void
testBuildDispatchString(DispatchCommandParms *pParms)
{
	pParms->query_text = malloc(100);
	*pParms->query_text = 'T';
	pParms->query_text_len = 100;
}

static void
testDispatchCommand(CdbDispatchResult *dispatchResult, DispatchCommandParms *pParms)
{
	SegmentDatabaseDescriptor *segdbDesc = dispatchResult->segdbDesc;

	if (DEBUG3 >= log_min_messages)
		write_log("%s <- dtx protocol command %d", segdbDesc->whoami, (int)pParms->dtxProtocolParms.dtxProtocolCommand);

	dispatchCommand(dispatchResult, pParms->query_text, pParms->query_text_len);
}

static void
testDispatchDestroy(DispatchCommandParms *pParms)
{
}

static void
testDispatchInit(DispatchCommandParms *pParms, void *inputParms)
{
}

Gang *mockCreateGang()
{
	Gang *mockGang = malloc(sizeof(Gang));
	mockGang->type = GANGTYPE_PRIMARY_WRITER;
	mockGang->size = 10;
	return mockGang;
}

int main()
{
	main_tid = pthread_self();

	int nSlices = 10;
	struct CdbDispatcherState ds;
	Gang *mockGang = mockCreateGang();

	ds.primaryResults = cdbdisp_makeDispatchResults(nSlices * largestGangsize(),
												   10,
												   true);
	ds.dispatchThreads = NULL;

	cdbdisp_dispatchToGang(&ds, &TestDispatchType, NULL, mockGang, 0, 10, NULL);

}
