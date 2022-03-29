/*-------------------------------------------------------------------------
 *
 * mysql_fdw.c
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/*
 * Must be included before mysql.h as it has some conflicting definitions like
 * list_length, etc.
 */
#include "mysql_fdw.h"

#include <dlfcn.h>
#include <errmsg.h>
#include <mysql.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "commands/defrem.h"
#include "commands/explain.h"
#include "catalog/heap.h"
#include "catalog/pg_type.h"
#include "foreign/fdwapi.h"
#include "miscadmin.h"
#include "mysql_query.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

/* Declarations for dynamic loading */
PG_MODULE_MAGIC;

int ((mysql_options) (MYSQL *mysql, enum mysql_option option,
					  const void *arg));
int ((mysql_stmt_prepare) (MYSQL_STMT *stmt, const char *query,
						   unsigned long length));
int ((mysql_stmt_execute) (MYSQL_STMT *stmt));
int ((mysql_stmt_fetch) (MYSQL_STMT *stmt));
int ((mysql_query) (MYSQL *mysql, const char *q));
bool ((mysql_stmt_attr_set) (MYSQL_STMT *stmt,
							 enum enum_stmt_attr_type attr_type,
							 const void *attr));
bool ((mysql_stmt_close) (MYSQL_STMT *stmt));
bool ((mysql_stmt_reset) (MYSQL_STMT *stmt));
bool ((mysql_free_result) (MYSQL_RES *result));
bool ((mysql_stmt_bind_param) (MYSQL_STMT *stmt, MYSQL_BIND *bnd));
bool ((mysql_stmt_bind_result) (MYSQL_STMT *stmt, MYSQL_BIND *bnd));

MYSQL_STMT *((mysql_stmt_init) (MYSQL *mysql));
MYSQL_RES *((mysql_stmt_result_metadata) (MYSQL_STMT *stmt));
int ((mysql_stmt_store_result) (MYSQL_STMT *stmt));
MYSQL_ROW((mysql_fetch_row) (MYSQL_RES *result));
MYSQL_FIELD *((mysql_fetch_field) (MYSQL_RES *result));
MYSQL_FIELD *((mysql_fetch_fields) (MYSQL_RES *result));
const char *((mysql_error) (MYSQL *mysql));
void ((mysql_close) (MYSQL *sock));
MYSQL_RES *((mysql_store_result) (MYSQL *mysql));
MYSQL *((mysql_init) (MYSQL *mysql));
bool ((mysql_ssl_set) (MYSQL *mysql, const char *key, const char *cert,
					   const char *ca, const char *capath,
					   const char *cipher));
MYSQL *((mysql_real_connect) (MYSQL *mysql, const char *host, const char *user,
							  const char *passwd, const char *db,
							  unsigned int port, const char *unix_socket,
							  unsigned long clientflag));

const char *((mysql_get_host_info) (MYSQL *mysql));
const char *((mysql_get_server_info) (MYSQL *mysql));
int ((mysql_get_proto_info) (MYSQL *mysql));

unsigned int ((mysql_stmt_errno) (MYSQL_STMT *stmt));
unsigned int ((mysql_errno) (MYSQL *mysql));
unsigned int ((mysql_num_fields) (MYSQL_RES *result));
unsigned int ((mysql_num_rows) (MYSQL_RES *result));

#define DEFAULTE_NUM_ROWS    1000

/*
 * In PG 9.5.1 the number will be 90501,
 * our version is 2.7.0 so number will be 20700
 */
#define CODE_VERSION   20700

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum mysqlFdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, mysqlFdwScanPrivateSelectSql));
 */
enum mysqlFdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	mysqlFdwScanPrivateSelectSql,

	/* Integer list of attribute numbers retrieved by the SELECT */
	mysqlFdwScanPrivateRetrievedAttrs,

	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	mysqlFdwScanPrivateRelations,

	/*
	 * List of Var node lists for constructing the whole-row references of
	 * base relations involved in pushed down join.
	 */
	mysqlFdwPrivateWholeRowLists,

	/*
	 * Targetlist representing the result fetched from the foreign server if
	 * whole-row references are involved.
	 */
	mysqlFdwPrivateScanTList
};

extern PGDLLEXPORT void _PG_init(void);
extern Datum mysql_fdw_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mysql_fdw_handler);
PG_FUNCTION_INFO_V1(mysql_fdw_version);

/*
 * FDW callback routines
 */
static void mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void mysqlBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *mysqlIterateForeignScan(ForeignScanState *node);
static void mysqlReScanForeignScan(ForeignScanState *node);
static void mysqlEndForeignScan(ForeignScanState *node);

static List *mysqlPlanForeignModify(PlannerInfo *root, ModifyTable *plan,
									Index resultRelation, int subplan_index);
static void mysqlBeginForeignModify(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo,
									List *fdw_private, int subplan_index,
									int eflags);
static TupleTableSlot *mysqlExecForeignInsert(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
#if PG_VERSION_NUM >= 140000
static void mysqlAddForeignUpdateTargets(PlannerInfo *root,
										 Index rtindex,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
#else
static void mysqlAddForeignUpdateTargets(Query *parsetree,
										 RangeTblEntry *target_rte,
										 Relation target_relation);
#endif
static TupleTableSlot *mysqlExecForeignUpdate(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static TupleTableSlot *mysqlExecForeignDelete(EState *estate,
											  ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot,
											  TupleTableSlot *planSlot);
static void mysqlEndForeignModify(EState *estate,
								  ResultRelInfo *resultRelInfo);

static void mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								   Oid foreigntableid);
static void mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								 Oid foreigntableid);
static bool mysqlAnalyzeForeignTable(Relation relation,
									 AcquireSampleRowsFunc *func,
									 BlockNumber *totalpages);
static ForeignScan *mysqlGetForeignPlan(PlannerInfo *root,
										RelOptInfo *foreignrel,
										Oid foreigntableid,
										ForeignPath *best_path, List *tlist,
										List *scan_clauses, Plan *outer_plan);
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel,
							   Cost *startup_cost, Cost *total_cost,
							   Oid foreigntableid);
static void mysqlGetForeignJoinPaths(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *outerrel,
									 RelOptInfo *innerrel,
									 JoinType jointype,
									 JoinPathExtraData *extra);
static bool mysqlRecheckForeignScan(ForeignScanState *node,
									TupleTableSlot *slot);

#if PG_VERSION_NUM >= 110000
static void mysqlGetForeignUpperPaths(PlannerInfo *root,
									  UpperRelationKind stage,
									  RelOptInfo *input_rel,
									  RelOptInfo *output_rel,
									  void *extra);
#elif PG_VERSION_NUM >= 100000
static void mysqlGetForeignUpperPaths(PlannerInfo *root,
									  UpperRelationKind stage,
									  RelOptInfo *input_rel,
									  RelOptInfo *output_rel);
#endif
static List *mysqlImportForeignSchema(ImportForeignSchemaStmt *stmt,
									  Oid serverOid);

#if PG_VERSION_NUM >= 110000
static void mysqlBeginForeignInsert(ModifyTableState *mtstate,
									ResultRelInfo *resultRelInfo);
static void mysqlEndForeignInsert(EState *estate,
								  ResultRelInfo *resultRelInfo);
#endif

/*
 * Helper functions
 */
bool mysql_load_library(void);
static void mysql_fdw_exit(int code, Datum arg);
static bool mysql_is_column_unique(Oid foreigntableid);

static void prepare_query_params(PlanState *node,
								 List *fdw_exprs,
								 int numParams,
								 FmgrInfo **param_flinfo,
								 List **param_exprs,
								 const char ***param_values,
								 Oid **param_types);

static void process_query_params(ExprContext *econtext,
								 FmgrInfo *param_flinfo,
								 List *param_exprs,
								 const char **param_values,
								 MYSQL_BIND **mysql_bind_buf,
								 Oid *param_types);

static void bind_stmt_params_and_exec(ForeignScanState *node);
static bool mysql_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
								  JoinType jointype, RelOptInfo *outerrel,
								  RelOptInfo *innerrel,
								  JoinPathExtraData *extra);
static List *mysql_adjust_whole_row_ref(PlannerInfo *root,
										List *scan_var_list,
										List **whole_row_lists,
										Bitmapset *relids);
static List *mysql_build_scan_list_for_baserel(Oid relid, Index varno,
											   Bitmapset *attrs_used,
											   List **retrieved_attrs);
static void mysql_build_whole_row_constr_info(MySQLFdwExecState *festate,
											  TupleDesc tupdesc,
											  Bitmapset *relids,
											  int max_relid,
											  List *whole_row_lists,
											  List *scan_tlist,
											  List *fdw_scan_tlist);
static HeapTuple mysql_get_tuple_with_whole_row(MySQLFdwExecState *festate,
												Datum *values,bool *nulls);
static HeapTuple mysql_form_whole_row(MySQLWRState *wr_state, Datum *values,
									  bool *nulls);
#if PG_VERSION_NUM >= 110000
static bool mysql_foreign_grouping_ok(PlannerInfo *root,
									  RelOptInfo *grouped_rel,
									  Node *havingQual);
static void mysql_add_foreign_grouping_paths(PlannerInfo *root,
											 RelOptInfo *input_rel,
											 RelOptInfo *grouped_rel,
											 GroupPathExtraData *extra);
#elif PG_VERSION_NUM >= 100000
static bool mysql_foreign_grouping_ok(PlannerInfo *root,
									  RelOptInfo *grouped_rel);
static void mysql_add_foreign_grouping_paths(PlannerInfo *root,
											 RelOptInfo *input_rel,
											 RelOptInfo *grouped_rel);
#endif

void *mysql_dll_handle = NULL;
static int wait_timeout = WAIT_TIMEOUT;
static int interactive_timeout = INTERACTIVE_TIMEOUT;
static void mysql_error_print(MYSQL *conn);
static void mysql_stmt_error_print(MySQLFdwExecState *festate,
								   const char *msg);
static List *getUpdateTargetAttrs(RangeTblEntry *rte);

/*
 * mysql_load_library function dynamically load the mysql's library
 * libmysqlclient.so. The only reason to load the library using dlopen
 * is that, mysql and postgres both have function with same name like
 * "list_delete", "list_delete" and "list_free" which cause compiler
 * error "duplicate function name" and erroneously linking with a function.
 * This port of the code is used to avoid the compiler error.
 *
 * #define list_delete mysql_list_delete
 * #include <mysql.h>
 * #undef list_delete
 *
 * But system crashed on function mysql_stmt_close function because
 * mysql_stmt_close internally calling "list_delete" function which
 * wrongly binds to postgres' "list_delete" function.
 *
 * The dlopen function provides a parameter "RTLD_DEEPBIND" which
 * solved the binding issue.
 *
 * RTLD_DEEPBIND:
 * Place the lookup scope of the symbols in this library ahead of the
 * global scope. This means that a self-contained library will use its
 * own symbols in preference to global symbols with the same name contained
 * in libraries that have already been loaded.
 */
bool
mysql_load_library(void)
{
#if defined(__APPLE__) || defined(__FreeBSD__)
	/*
	 * Mac OS/BSD does not support RTLD_DEEPBIND, but it still works without
	 * the RTLD_DEEPBIND
	 */
	mysql_dll_handle = dlopen(_MYSQL_LIBNAME, RTLD_LAZY);
#else
	mysql_dll_handle = dlopen(_MYSQL_LIBNAME, RTLD_LAZY | RTLD_DEEPBIND);
#endif
	if (mysql_dll_handle == NULL)
		return false;

	_mysql_stmt_bind_param = dlsym(mysql_dll_handle, "mysql_stmt_bind_param");
	_mysql_stmt_bind_result = dlsym(mysql_dll_handle, "mysql_stmt_bind_result");
	_mysql_stmt_init = dlsym(mysql_dll_handle, "mysql_stmt_init");
	_mysql_stmt_prepare = dlsym(mysql_dll_handle, "mysql_stmt_prepare");
	_mysql_stmt_execute = dlsym(mysql_dll_handle, "mysql_stmt_execute");
	_mysql_stmt_fetch = dlsym(mysql_dll_handle, "mysql_stmt_fetch");
	_mysql_query = dlsym(mysql_dll_handle, "mysql_query");
	_mysql_stmt_result_metadata = dlsym(mysql_dll_handle, "mysql_stmt_result_metadata");
	_mysql_stmt_store_result = dlsym(mysql_dll_handle, "mysql_stmt_store_result");
	_mysql_fetch_row = dlsym(mysql_dll_handle, "mysql_fetch_row");
	_mysql_fetch_field = dlsym(mysql_dll_handle, "mysql_fetch_field");
	_mysql_fetch_fields = dlsym(mysql_dll_handle, "mysql_fetch_fields");
	_mysql_stmt_close = dlsym(mysql_dll_handle, "mysql_stmt_close");
	_mysql_stmt_reset = dlsym(mysql_dll_handle, "mysql_stmt_reset");
	_mysql_free_result = dlsym(mysql_dll_handle, "mysql_free_result");
	_mysql_error = dlsym(mysql_dll_handle, "mysql_error");
	_mysql_options = dlsym(mysql_dll_handle, "mysql_options");
	_mysql_ssl_set = dlsym(mysql_dll_handle, "mysql_ssl_set");
	_mysql_real_connect = dlsym(mysql_dll_handle, "mysql_real_connect");
	_mysql_close = dlsym(mysql_dll_handle, "mysql_close");
	_mysql_init = dlsym(mysql_dll_handle, "mysql_init");
	_mysql_stmt_attr_set = dlsym(mysql_dll_handle, "mysql_stmt_attr_set");
	_mysql_store_result = dlsym(mysql_dll_handle, "mysql_store_result");
	_mysql_stmt_errno = dlsym(mysql_dll_handle, "mysql_stmt_errno");
	_mysql_errno = dlsym(mysql_dll_handle, "mysql_errno");
	_mysql_num_fields = dlsym(mysql_dll_handle, "mysql_num_fields");
	_mysql_num_rows = dlsym(mysql_dll_handle, "mysql_num_rows");
	_mysql_get_host_info = dlsym(mysql_dll_handle, "mysql_get_host_info");
	_mysql_get_server_info = dlsym(mysql_dll_handle, "mysql_get_server_info");
	_mysql_get_proto_info = dlsym(mysql_dll_handle, "mysql_get_proto_info");

	if (_mysql_stmt_bind_param == NULL ||
		_mysql_stmt_bind_result == NULL ||
		_mysql_stmt_init == NULL ||
		_mysql_stmt_prepare == NULL ||
		_mysql_stmt_execute == NULL ||
		_mysql_stmt_fetch == NULL ||
		_mysql_query == NULL ||
		_mysql_stmt_result_metadata == NULL ||
		_mysql_stmt_store_result == NULL ||
		_mysql_fetch_row == NULL ||
		_mysql_fetch_field == NULL ||
		_mysql_fetch_fields == NULL ||
		_mysql_stmt_close == NULL ||
		_mysql_stmt_reset == NULL ||
		_mysql_free_result == NULL ||
		_mysql_error == NULL ||
		_mysql_options == NULL ||
		_mysql_ssl_set == NULL ||
		_mysql_real_connect == NULL ||
		_mysql_close == NULL ||
		_mysql_init == NULL ||
		_mysql_stmt_attr_set == NULL ||
		_mysql_store_result == NULL ||
		_mysql_stmt_errno == NULL ||
		_mysql_errno == NULL ||
		_mysql_num_fields == NULL ||
		_mysql_num_rows == NULL ||
		_mysql_get_host_info == NULL ||
		_mysql_get_server_info == NULL ||
		_mysql_get_proto_info == NULL)
		return false;

	return true;
}

/*
 * Library load-time initialization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void
_PG_init(void)
{
	if (!mysql_load_library())
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to load the mysql query: \n%s", dlerror()),
				 errhint("Export LD_LIBRARY_PATH to locate the library.")));

	DefineCustomIntVariable("mysql_fdw.wait_timeout",
							"Server-side wait_timeout",
							"Set the maximum wait_timeout"
							"use to set the MySQL session timeout",
							&wait_timeout,
							WAIT_TIMEOUT,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("mysql_fdw.interactive_timeout",
							"Server-side interactive timeout",
							"Set the maximum interactive timeout"
							"use to set the MySQL session timeout",
							&interactive_timeout,
							INTERACTIVE_TIMEOUT,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	on_proc_exit(&mysql_fdw_exit, PointerGetDatum(NULL));
}

/*
 * mysql_fdw_exit
 * 		Exit callback function.
 */
static void
mysql_fdw_exit(int code, Datum arg)
{
	mysql_cleanup_connection();
}

/*
 * Foreign-data wrapper handler function: return
 * a struct with pointers to my callback routines.
 */
Datum
mysql_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	fdwroutine->GetForeignRelSize = mysqlGetForeignRelSize;
	fdwroutine->GetForeignPaths = mysqlGetForeignPaths;
	fdwroutine->GetForeignPlan = mysqlGetForeignPlan;
	fdwroutine->BeginForeignScan = mysqlBeginForeignScan;
	fdwroutine->IterateForeignScan = mysqlIterateForeignScan;
	fdwroutine->ReScanForeignScan = mysqlReScanForeignScan;
	fdwroutine->EndForeignScan = mysqlEndForeignScan;

	/* Functions for updating foreign tables */
	fdwroutine->AddForeignUpdateTargets = mysqlAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = mysqlPlanForeignModify;
	fdwroutine->BeginForeignModify = mysqlBeginForeignModify;
	fdwroutine->ExecForeignInsert = mysqlExecForeignInsert;
	fdwroutine->ExecForeignUpdate = mysqlExecForeignUpdate;
	fdwroutine->ExecForeignDelete = mysqlExecForeignDelete;
	fdwroutine->EndForeignModify = mysqlEndForeignModify;

	/* Function for EvalPlanQual rechecks */
	fdwroutine->RecheckForeignScan = mysqlRecheckForeignScan;

	/* Support functions for EXPLAIN */
	fdwroutine->ExplainForeignScan = mysqlExplainForeignScan;

	/* Support functions for ANALYZE */
	fdwroutine->AnalyzeForeignTable = mysqlAnalyzeForeignTable;

	/* Support functions for IMPORT FOREIGN SCHEMA */
	fdwroutine->ImportForeignSchema = mysqlImportForeignSchema;

#if PG_VERSION_NUM >= 110000
	/* Partition routing and/or COPY from */
	fdwroutine->BeginForeignInsert = mysqlBeginForeignInsert;
	fdwroutine->EndForeignInsert = mysqlEndForeignInsert;
#endif

	/* Support functions for join push-down */
	fdwroutine->GetForeignJoinPaths = mysqlGetForeignJoinPaths;

	/* Support functions for upper relation push-down */
	fdwroutine->GetForeignUpperPaths = mysqlGetForeignUpperPaths;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * mysqlBeginForeignScan
 * 		Initiate access to the database
 */
static void
mysqlBeginForeignScan(ForeignScanState *node, int eflags)
{
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	MYSQL	   *conn;
	RangeTblEntry *rte;
	MySQLFdwExecState *festate;
	EState	   *estate = node->ss.ps.state;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	mysql_opt  *options;
	ListCell   *lc;
	int			atindex = 0;
	unsigned long type = (unsigned long) CURSOR_TYPE_READ_ONLY;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;
	char		timeout[255];
	int			numParams;
	int			rtindex;
	List	   *fdw_private = fsplan->fdw_private;
	char		sql_mode[255];

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case. node->fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */
	festate = (MySQLFdwExecState *) palloc(sizeof(MySQLFdwExecState));
	node->fdw_state = (void *) festate;

	/*
	 * If whole-row references are involved in pushed down join extract the
	 * information required to construct those.
	 */
	if (list_length(fdw_private) >= mysqlFdwPrivateScanTList)
	{
		List	   *whole_row_lists = list_nth(fdw_private,
											   mysqlFdwPrivateWholeRowLists);
		List	   *scan_tlist = list_nth(fdw_private,
										  mysqlFdwPrivateScanTList);
#if PG_VERSION_NUM >= 120000
		TupleDesc	scan_tupdesc = ExecTypeFromTL(scan_tlist);
#else
		TupleDesc	scan_tupdesc = ExecTypeFromTL(scan_tlist, false);
#endif

		mysql_build_whole_row_constr_info(festate, tupleDescriptor,
										  fsplan->fs_relids,
										  list_length(node->ss.ps.state->es_range_table),
										  whole_row_lists, scan_tlist,
										  fsplan->fdw_scan_tlist);

		/* Change tuple descriptor to match the result from foreign server. */
		tupleDescriptor = scan_tupdesc;
	}

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join use the lowest-numbered
	 * member RTE as a representative; we would get the same result from any.
	 */
	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = rt_fetch(rtindex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(rte->relid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch the options */
	options = mysql_get_options(rte->relid, true);

	/*
	 * Get the already connected connection, otherwise connect and get the
	 * connection handle.
	 */
	conn = mysql_get_connection(server, user, options);

	/* Stash away the state info we have already */
	festate->query = strVal(list_nth(fsplan->fdw_private,
									 mysqlFdwScanPrivateSelectSql));
	festate->retrieved_attrs = list_nth(fsplan->fdw_private,
										mysqlFdwScanPrivateRetrievedAttrs);
	festate->conn = conn;
	festate->query_executed = false;
	festate->has_var_size_col = false;
	festate->attinmeta = TupleDescGetAttInMetadata(tupleDescriptor);

#if PG_VERSION_NUM >= 110000
	festate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_DEFAULT_SIZES);
#else
	festate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);
#endif

	if (wait_timeout > 0)
	{
		/* Set the session timeout in seconds */
		sprintf(timeout, "SET wait_timeout = %d", wait_timeout);
		mysql_query(festate->conn, timeout);
	}

	if (interactive_timeout > 0)
	{
		/* Set the session timeout in seconds */
		sprintf(timeout, "SET interactive_timeout = %d", interactive_timeout);
		mysql_query(festate->conn, timeout);
	}

	snprintf(sql_mode, sizeof(sql_mode), "SET sql_mode = '%s'",
			 options->sql_mode);
	if (mysql_query(festate->conn, sql_mode) != 0)
		mysql_error_print(festate->conn);

	/* Initialize the MySQL statement */
	festate->stmt = mysql_stmt_init(festate->conn);
	if (festate->stmt == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to initialize the mysql query: \n%s",
						mysql_error(festate->conn))));

	/* Prepare MySQL statement */
	if (mysql_stmt_prepare(festate->stmt, festate->query,
						   strlen(festate->query)) != 0)
		mysql_stmt_error_print(festate, "failed to prepare the MySQL query");

	/* Prepare for output conversion of parameters used in remote query. */
	numParams = list_length(fsplan->fdw_exprs);
	festate->numParams = numParams;
	if (numParams > 0)
		prepare_query_params((PlanState *) node,
							 fsplan->fdw_exprs,
							 numParams,
							 &festate->param_flinfo,
							 &festate->param_exprs,
							 &festate->param_values,
							 &festate->param_types);

	/* int column_count = mysql_num_fields(festate->meta); */

	/* Set the statement as cursor type */
	mysql_stmt_attr_set(festate->stmt, STMT_ATTR_CURSOR_TYPE, (void *) &type);

	/* Set the pre-fetch rows */
	mysql_stmt_attr_set(festate->stmt, STMT_ATTR_PREFETCH_ROWS,
						(void *) &options->fetch_size);

	festate->table = (mysql_table *) palloc0(sizeof(mysql_table));
	festate->table->column = (mysql_column *) palloc0(sizeof(mysql_column) * tupleDescriptor->natts);
	festate->table->mysql_bind = (MYSQL_BIND *) palloc0(sizeof(MYSQL_BIND) * tupleDescriptor->natts);

	festate->table->mysql_res = mysql_stmt_result_metadata(festate->stmt);
	if (NULL == festate->table->mysql_res)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to retrieve query result set metadata: \n%s",
						mysql_error(festate->conn))));

	festate->table->mysql_fields = mysql_fetch_fields(festate->table->mysql_res);

	foreach(lc, festate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
		int32		pgtypmod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;

		if (TupleDescAttr(tupleDescriptor, attnum)->attisdropped)
			continue;

		if (pgtype == TEXTOID)
			festate->has_var_size_col = true;

		festate->table->column[atindex].mysql_bind = &festate->table->mysql_bind[atindex];

		mysql_bind_result(pgtype, pgtypmod,
						  &festate->table->mysql_fields[atindex],
						  &festate->table->column[atindex]);
		atindex++;
	}

	/*
	 * Set STMT_ATTR_UPDATE_MAX_LENGTH so that mysql_stmt_store_result() can
	 * update metadata MYSQL_FIELD->max_length value, this will be useful to
	 * determine var length column size.
	 */
	mysql_stmt_attr_set(festate->stmt, STMT_ATTR_UPDATE_MAX_LENGTH,
						&festate->has_var_size_col);

	/* Bind the results pointers for the prepare statements */
	if (mysql_stmt_bind_result(festate->stmt, festate->table->mysql_bind) != 0)
		mysql_stmt_error_print(festate, "failed to bind the MySQL query");
}

/*
 * mysqlIterateForeignScan
 * 		Iterate and get the rows one by one from  MySQL and placed in tuple
 * 		slot
 */
static TupleTableSlot *
mysqlIterateForeignScan(ForeignScanState *node)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;
	TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
	int			attid;
	ListCell   *lc;
	int			rc = 0;
	Datum	   *dvalues;
	bool	   *nulls;
	int			natts;
	AttInMetadata *attinmeta = festate->attinmeta;
	HeapTuple	tup;
	int			i;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	List	   *fdw_private = fsplan->fdw_private;

	natts = attinmeta->tupdesc->natts;

	dvalues = palloc0(natts * sizeof(Datum));
	nulls = palloc(natts * sizeof(bool));
	/* Initialize to nulls for any columns not present in result */
	memset(nulls, true, natts * sizeof(bool));

	ExecClearTuple(tupleSlot);

	/*
	 * If this is the first call after Begin or ReScan, we need to bind the
	 * params and execute the query.
	 */
	if (!festate->query_executed)
		bind_stmt_params_and_exec(node);

	attid = 0;
	rc = mysql_stmt_fetch(festate->stmt);
	if (rc == 0)
	{
		foreach(lc, festate->retrieved_attrs)
		{
			int			attnum = lfirst_int(lc) - 1;
			Oid			pgtype = TupleDescAttr(attinmeta->tupdesc, attnum)->atttypid;
			int32		pgtypmod = TupleDescAttr(attinmeta->tupdesc, attnum)->atttypmod;

			nulls[attnum] = festate->table->column[attid].is_null;
			if (!festate->table->column[attid].is_null)
				dvalues[attnum] = mysql_convert_to_pg(pgtype, pgtypmod,
													  &festate->table->column[attid]);

			attid++;
		}

		ExecClearTuple(tupleSlot);

		if (list_length(fdw_private) >= mysqlFdwPrivateScanTList)
		{
			/* Construct tuple with whole-row references. */
			tup = mysql_get_tuple_with_whole_row(festate, dvalues, nulls);
		}
		else
		{
			/* Form the Tuple using Datums */
			tup = heap_form_tuple(attinmeta->tupdesc, dvalues, nulls);
		}

		if (tup)
#if PG_VERSION_NUM >= 120000
			ExecStoreHeapTuple(tup, tupleSlot, false);
#else
			ExecStoreTuple(tup, tupleSlot, InvalidBuffer, false);
#endif
		else
			mysql_stmt_close(festate->stmt);

		/*
		 * Release locally palloc'd space and values of pass-by-reference
		 * datums, as well.
		 */
		for (i = 0; i < natts; i++)
		{
			if (dvalues[i] && !TupleDescAttr(attinmeta->tupdesc, i)->attbyval)
				pfree(DatumGetPointer(dvalues[i]));
		}
		pfree(dvalues);
		pfree(nulls);
	}
	else if (rc == 1)
	{
		/*
		 * Error occurred. Error code and message can be obtained by calling
		 * mysql_stmt_errno() and mysql_stmt_error().
		 */
	}
	else if (rc == MYSQL_NO_DATA)
	{
		/*
		 * No more rows/data exists
		 */
	}
	else if (rc == MYSQL_DATA_TRUNCATED)
	{
		/* Data truncation occurred */
		/*
		 * MYSQL_DATA_TRUNCATED is returned when truncation reporting is
		 * enabled. To determine which column values were truncated when this
		 * value is returned, check the error members of the MYSQL_BIND
		 * structures used for fetching values. Truncation reporting is
		 * enabled by default, but can be controlled by calling
		 * mysql_options() with the MYSQL_REPORT_DATA_TRUNCATION option.
		 */
	}

	return tupleSlot;
}


/*
 * mysqlExplainForeignScan
 * 		Produce extra output for EXPLAIN
 */
static void
mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	RangeTblEntry *rte;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	int			rtindex;
	EState	   *estate = node->ss.ps.state;
	List	   *fdw_private = fsplan->fdw_private;

	if (fsplan->scan.scanrelid > 0)
		rtindex = fsplan->scan.scanrelid;
	else
		rtindex = bms_next_member(fsplan->fs_relids, -1);
	rte = rt_fetch(rtindex, estate->es_range_table);

	if (list_length(fdw_private) > mysqlFdwScanPrivateRelations)
	{
		char	   *relations = strVal(list_nth(fdw_private,
												mysqlFdwScanPrivateRelations));

		ExplainPropertyText("Relations", relations, es);
	}

	/* Give some possibly useful info about startup costs */
	if (es->costs)
	{
		mysql_opt  *options = mysql_get_options(rte->relid, true);

		if (strcmp(options->svr_address, "127.0.0.1") == 0 ||
			strcmp(options->svr_address, "localhost") == 0)
#if PG_VERSION_NUM >= 110000
			ExplainPropertyInteger("Local server startup cost", NULL, 10, es);
#else
			ExplainPropertyLong("Local server startup cost", 10, es);
#endif
		else
#if PG_VERSION_NUM >= 110000
			ExplainPropertyInteger("Remote server startup cost", NULL, 25, es);
#else
			ExplainPropertyLong("Remote server startup cost", 25, es);
#endif
	}
	/* Show the remote query in verbose mode */
	if (es->verbose)
	{
		char	   *remote_sql = strVal(list_nth(fdw_private,
												 mysqlFdwScanPrivateSelectSql));

		ExplainPropertyText("Remote query", remote_sql, es);
	}
}

/*
 * mysqlEndForeignScan
 * 		Finish scanning foreign table and dispose objects used for this scan
 */
static void
mysqlEndForeignScan(ForeignScanState *node)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;

	/* if festate is NULL, we are in EXPLAIN; do nothing */
	if (festate == NULL)
		return;

	if (festate->table && festate->table->mysql_res)
	{
		mysql_free_result(festate->table->mysql_res);
		festate->table->mysql_res = NULL;
	}

	if (festate->stmt)
	{
		mysql_stmt_close(festate->stmt);
		festate->stmt = NULL;
	}
}

/*
 * mysqlReScanForeignScan
 * 		Rescan table, possibly with new parameters
 */
static void
mysqlReScanForeignScan(ForeignScanState *node)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;

	/*
	 * Set the query_executed flag to false so that the query will be executed
	 * in mysqlIterateForeignScan().
	 */
	festate->query_executed = false;

}

/*
 * mysqlGetForeignRelSize
 * 		Create a FdwPlan for a scan on the foreign table
 */
static void
mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
					   Oid foreigntableid)
{
	double		rows = 0;
	double		filtered = 0;
	MYSQL	   *conn;
	MYSQL_ROW	row;
	Bitmapset  *attrs_used = NULL;
	mysql_opt  *options;
	Oid			userid = GetUserId();
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;
	MySQLFdwRelationInfo *fpinfo;
	ListCell   *lc;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	const char *database;
	const char *relname;
	const char *refname;
	char		sql_mode[255];

	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	/* Base foreign tables need to be push down always. */
	fpinfo->pushdown_safe = true;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch options */
	options = mysql_get_options(foreigntableid, true);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	snprintf(sql_mode, sizeof(sql_mode), "SET sql_mode = '%s'",
			 options->sql_mode);
	if (mysql_query(conn, sql_mode) != 0)
		mysql_error_print(conn);

	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &attrs_used);

	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (mysql_is_foreign_expr(root, baserel, ri->clause, false))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
		else
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
	}

	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &fpinfo->attrs_used);

	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		pull_varattnos((Node *) rinfo->clause, baserel->relid,
					   &fpinfo->attrs_used);
	}

	if (options->use_remote_estimate)
	{
		StringInfoData sql;
		MYSQL_RES  *result = NULL;
		List	   *retrieved_attrs = NULL;

		initStringInfo(&sql);
		appendStringInfo(&sql, "EXPLAIN ");

		mysql_deparse_select_stmt_for_rel(&sql, root, baserel, NULL,
										  fpinfo->remote_conds,
										  &retrieved_attrs, NULL);

		if (mysql_query(conn, sql.data) != 0)
			mysql_error_print(conn);

		result = mysql_store_result(conn);
		if (result)
		{
			int			num_fields;

			/*
			 * MySQL provide numbers of rows per table invole in the statement,
			 * but we don't have problem with it because we are sending
			 * separate query per table in FDW.
			 */
			row = mysql_fetch_row(result);
			num_fields = mysql_num_fields(result);
			if (row)
			{
				MYSQL_FIELD *field;
				int			i;

				for (i = 0; i < num_fields; i++)
				{
					field = mysql_fetch_field(result);
					if (!row[i])
						continue;
					else if (strcmp(field->name, "rows") == 0)
						rows = atof(row[i]);
					else if (strcmp(field->name, "filtered") == 0)
						filtered = atof(row[i]);
				}
			}
			mysql_free_result(result);
		}
	}
	if (rows > 0)
		rows = ((rows + 1) * filtered) / 100;
	else
		rows = DEFAULTE_NUM_ROWS;

	baserel->rows = rows;
	baserel->tuples = rows;

	/*
	 * Set the name of relation in fpinfo, while we are constructing it here.
	 * It will be used to build the string describing the join relation in
	 * EXPLAIN output.  We can't know whether VERBOSE option is specified or
	 * not, so always schema-qualify the foreign table name.
	 */
	fpinfo->relation_name = makeStringInfo();
	database = options->svr_database;
	relname = get_rel_name(foreigntableid);
	refname = rte->eref->aliasname;
	appendStringInfo(fpinfo->relation_name, "%s.%s",
					 quote_identifier(database), quote_identifier(relname));
	if (*refname && strcmp(refname, relname) != 0)
		appendStringInfo(fpinfo->relation_name, " %s",
						 quote_identifier(rte->eref->aliasname));
}

static bool
mysql_is_column_unique(Oid foreigntableid)
{
	StringInfoData sql;
	MYSQL	   *conn;
	MYSQL_RES  *result;
	mysql_opt  *options;
	Oid			userid = GetUserId();
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch the options */
	options = mysql_get_options(foreigntableid, true);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	/* Build the query */
	initStringInfo(&sql);

	/*
	 * Construct the query by prefixing the database name so that it can lookup
	 * in correct database.
	 */
	appendStringInfo(&sql, "EXPLAIN %s.%s", options->svr_database,
					 options->svr_table);
	if (mysql_query(conn, sql.data) != 0)
		mysql_error_print(conn);

	result = mysql_store_result(conn);
	if (result)
	{
		int			num_fields = mysql_num_fields(result);
		MYSQL_ROW	row;

		row = mysql_fetch_row(result);
		if (row && num_fields > 3)
		{
			if ((strcmp(row[3], "PRI") == 0) || (strcmp(row[3], "UNI")) == 0)
			{
				mysql_free_result(result);
				return true;
			}
		}
		mysql_free_result(result);
	}

	return false;
}

/*
 * mysqlEstimateCosts
 * 		Estimate the remote query cost
 */
static void
mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost,
				   Cost *total_cost, Oid foreigntableid)
{
	mysql_opt  *options;

	/* Fetch options */
	options = mysql_get_options(foreigntableid, true);

	/* Local databases are probably faster */
	if (strcmp(options->svr_address, "127.0.0.1") == 0 ||
		strcmp(options->svr_address, "localhost") == 0)
		*startup_cost = 10;
	else
		*startup_cost = 25;

	*total_cost = baserel->rows + *startup_cost;
}

/*
 * mysqlGetForeignPaths
 * 		Get the foreign paths
 */
static void
mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					 Oid foreigntableid)
{
	Cost		startup_cost;
	Cost		total_cost;

	/* Estimate costs */
	mysqlEstimateCosts(root, baserel, &startup_cost, &total_cost,
					   foreigntableid);

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 baserel->lateral_relids,
									 NULL,	/* no extra plan */
									 NULL));	/* no fdw_private data */
}


/*
 * mysqlGetForeignPlan
 * 		Get a foreign scan plan node
 */
static ForeignScan *
mysqlGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignrel,
					Oid foreigntableid, ForeignPath *best_path,
					List *tlist, List *scan_clauses, Plan *outer_plan)
{
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) foreignrel->fdw_private;
	Index		scan_relid;
	List	   *fdw_private;
	List	   *local_exprs = NIL;
	List	   *params_list = NIL;
	List	   *remote_conds = NIL;
	StringInfoData sql;
	List	   *retrieved_attrs;
	ListCell   *lc;
	List	   *scan_var_list;
	List	   *fdw_scan_tlist = NIL;
	List	   *whole_row_lists = NIL;

	if (foreignrel->reloptkind == RELOPT_BASEREL ||
		foreignrel->reloptkind == RELOPT_OTHER_MEMBER_REL)
		scan_relid = foreignrel->relid;
	else
	{
		scan_relid = 0;
		Assert(!scan_clauses);

		remote_conds = fpinfo->remote_conds;
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
	}

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe are shown in fpinfo->remote_conds and
	 * fpinfo->local_conds.  Anything else in the scan_clauses list will be
	 * a join clause, which we have to check for remote-safety.
	 *
	 * This code must match "extract_actual_clauses(scan_clauses, false)"
	 * except for the additional decision about remote versus local execution.
	 * Note however that we only strip the RestrictInfo nodes from the
	 * local_exprs list, since appendWhereClause expects a list of
	 * RestrictInfos.
	 */
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		Assert(IsA(rinfo, RestrictInfo));

		/* Ignore any pseudoconstants, they're dealt with elsewhere */
		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(fpinfo->remote_conds, rinfo))
			remote_conds = lappend(remote_conds, rinfo);
		else if (list_member_ptr(fpinfo->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
		else if (mysql_is_foreign_expr(root, foreignrel, rinfo->clause, false))
			remote_conds = lappend(remote_conds, rinfo);
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	if (IS_UPPER_REL(foreignrel))
		scan_var_list = pull_var_clause((Node *) fpinfo->grouped_tlist,
										PVC_RECURSE_AGGREGATES);
	else
		scan_var_list = pull_var_clause((Node *) foreignrel->reltarget->exprs,
										PVC_RECURSE_PLACEHOLDERS);

	if (IS_JOIN_REL(foreignrel))
	{
		scan_var_list = list_concat_unique(NIL, scan_var_list);

		scan_var_list = list_concat_unique(scan_var_list,
										   pull_var_clause((Node *) local_exprs,
														   PVC_RECURSE_PLACEHOLDERS));

		/*
		 * For join relations, planner needs targetlist, which represents the
		 * output of ForeignScan node. Prepare this before we modify
		 * scan_var_list to include Vars required by whole row references, if
		 * any.  Note that base foreign scan constructs the whole-row reference
		 * at the time of projection.  Joins are required to get them from the
		 * underlying base relations.  For a pushed down join the underlying
		 * relations do not exist, hence the whole-row references need to be
		 * constructed separately.
		 */
		fdw_scan_tlist = add_to_flat_tlist(NIL, scan_var_list);

		/*
		 * MySQL does not allow row value constructors to be part of SELECT
		 * list.  Hence, whole row reference in join relations need to be
		 * constructed by combining all the attributes of required base
		 * relations into a tuple after fetching the result from the foreign
		 * server.  So adjust the targetlist to include all attributes for
		 * required base relations.  The function also returns list of Var node
		 * lists required to construct the whole-row references of the
		 * involved relations.
		 */
		scan_var_list = mysql_adjust_whole_row_ref(root, scan_var_list,
												   &whole_row_lists,
												   foreignrel->relids);

		if (outer_plan)
		{
			ListCell   *lc;

			/*
			 * Right now, we only consider grouping and aggregation beyond
			 * joins.  Queries involving aggregates or grouping do not require
			 * EPQ mechanism, hence should not have an outer plan here.
			 */
			Assert(!IS_UPPER_REL(foreignrel));

			foreach(lc, local_exprs)
			{
				Node	   *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.  (They might also be
				 * in the mergequals or hashquals, but we can't touch those
				 * without breaking the plan.)
				 */
				if (IsA(outer_plan, NestLoop) ||
					IsA(outer_plan, MergeJoin) ||
					IsA(outer_plan, HashJoin))
				{
					Join	   *join_plan = (Join *) outer_plan;

					if (join_plan->jointype == JOIN_INNER)
						join_plan->joinqual = list_delete(join_plan->joinqual,
														  qual);
				}
			}
		}
	}
	else if (IS_UPPER_REL(foreignrel))
	{
		/*
		 * scan_var_list should have expressions and not TargetEntry nodes.
		 * However grouped_tlist created has TLEs, thus retrieve them into
		 * scan_var_list.
		 */
		scan_var_list = list_concat_unique(NIL,
										   get_tlist_exprs(fpinfo->grouped_tlist,
														   false));

		/*
		 * The targetlist computed while assessing push-down safety represents
		 * the result we expect from the foreign server.
		 */
		fdw_scan_tlist = fpinfo->grouped_tlist;
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	mysql_deparse_select_stmt_for_rel(&sql, root, foreignrel, scan_var_list,
									  remote_conds, &retrieved_attrs,
									  &params_list);

#if PG_VERSION_NUM >= 140000
	if (bms_is_member(foreignrel->relid, root->all_result_relids) &&
#else
	if (foreignrel->relid == root->parse->resultRelation &&
#endif
		(root->parse->commandType == CMD_UPDATE ||
		 root->parse->commandType == CMD_DELETE))
	{
		/* Relation is UPDATE/DELETE target, so use FOR UPDATE */
		appendStringInfoString(&sql, " FOR UPDATE");
	}

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwScanPrivateIndex, above.
	 */

	fdw_private = list_make2(makeString(sql.data), retrieved_attrs);
	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		fdw_private = lappend(fdw_private,
							  makeString(fpinfo->relation_name->data));

		/*
		 * To construct whole row references we need:
		 *
		 * 1. The lists of Var nodes required for whole-row references of
		 *    joining relations
		 * 2. targetlist corresponding the result expected from the foreign
		 *    server.
		 */
		if (whole_row_lists)
		{
			fdw_private = lappend(fdw_private, whole_row_lists);
			fdw_private = lappend(fdw_private,
								  add_to_flat_tlist(NIL, scan_var_list));
		}
	}

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist, local_exprs, scan_relid, params_list,
							fdw_private, fdw_scan_tlist, NIL, outer_plan);
}

/*
 * mysqlAnalyzeForeignTable
 * 		Implement stats collection
 */
static bool
mysqlAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func,
						 BlockNumber *totalpages)
{
	StringInfoData sql;
	double		table_size = 0;
	MYSQL	   *conn;
	MYSQL_RES  *result;
	Oid			foreignTableId = RelationGetRelid(relation);
	mysql_opt  *options;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(relation->rd_rel->relowner, server->serverid);

	/* Fetch options */
	options = mysql_get_options(foreignTableId, true);
	Assert(options->svr_database != NULL && options->svr_table != NULL);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	/* Build the query */
	initStringInfo(&sql);
	mysql_deparse_analyze(&sql, options->svr_database, options->svr_table);

	if (mysql_query(conn, sql.data) != 0)
		mysql_error_print(conn);

	result = mysql_store_result(conn);

	/*
	 * To get the table size in ANALYZE operation, we run a SELECT query by
	 * passing the database name and table name.  So if the remote table is not
	 * present, then we end up getting zero rows.  Throw an error in that case.
	 */
	if (mysql_num_rows(result) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
				 errmsg("relation %s.%s does not exist", options->svr_database,
						options->svr_table)));

	if (result)
	{
		MYSQL_ROW	row;

		row = mysql_fetch_row(result);
		table_size = atof(row[0]);
		mysql_free_result(result);
	}

	*totalpages = table_size / MYSQL_BLKSIZ;

	return false;
}

static List *
mysqlPlanForeignModify(PlannerInfo *root,
					   ModifyTable *plan,
					   Index resultRelation,
					   int subplan_index)
{

	CmdType		operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation	rel;
	List	   *targetAttrs = NIL;
	StringInfoData sql;
	char	   *attname;
	Oid			foreignTableId;

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
#if PG_VERSION_NUM < 130000
	rel = heap_open(rte->relid, NoLock);
#else
	rel = table_open(rte->relid, NoLock);
#endif

	foreignTableId = RelationGetRelid(rel);

	if (!mysql_is_column_unique(foreignTableId))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("first column of remote table must be unique for INSERT/UPDATE/DELETE operation")));

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
	 * foreign table, we transmit all columns like INSERT; else we transmit
	 * only columns that were explicitly targets of the UPDATE, so as to avoid
	 * unnecessary data transmission.  (We can't do that for INSERT since we
	 * would miss sending default values for columns not listed in the source
	 * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
	 * those triggers might change values for non-target columns, in which
	 * case we would miss sending changed values for those columns.)
	 */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		TupleDesc	tupdesc = RelationGetDescr(rel);
		int			attnum;

		/*
		 * If it is an UPDATE operation, check for row identifier column in
		 * target attribute list by calling getUpdateTargetAttrs().
		 */
		if (operation == CMD_UPDATE)
			getUpdateTargetAttrs(rte);

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		targetAttrs = getUpdateTargetAttrs(rte);
		/* We also want the rowid column to be available for the update */
		targetAttrs = lcons_int(1, targetAttrs);
	}
	else
		targetAttrs = lcons_int(1, targetAttrs);

#if PG_VERSION_NUM >= 110000
	attname = get_attname(foreignTableId, 1, false);
#else
	attname = get_relid_attribute_name(foreignTableId, 1);
#endif

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			mysql_deparse_insert(&sql, root, resultRelation, rel, targetAttrs);
			break;
		case CMD_UPDATE:
			mysql_deparse_update(&sql, root, resultRelation, rel, targetAttrs,
								 attname);
			break;
		case CMD_DELETE:
			mysql_deparse_delete(&sql, root, resultRelation, rel, attname);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	if (plan->returningLists)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("RETURNING is not supported by this FDW")));

#if PG_VERSION_NUM < 130000
	heap_close(rel, NoLock);
#else
	table_close(rel, NoLock);
#endif

	return list_make2(makeString(sql.data), targetAttrs);
}

/*
 * mysqlBeginForeignModify
 * 		Begin an insert/update/delete operation on a foreign table
 */
static void
mysqlBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	MySQLFdwExecState *fmstate;
	EState	   *estate = mtstate->ps.state;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	AttrNumber	n_params;
	Oid			typefnoid = InvalidOid;
	bool		isvarlena = false;
	ListCell   *lc;
	Oid			foreignTableId = InvalidOid;
	RangeTblEntry *rte;
	Oid			userid;
	ForeignServer *server;
	UserMapping *user;
	ForeignTable *table;

	rte = rt_fetch(resultRelInfo->ri_RangeTableIndex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	foreignTableId = RelationGetRelid(rel);

	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case. resultRelInfo->ri_FdwState
	 * stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Begin constructing MySQLFdwExecState. */
	fmstate = (MySQLFdwExecState *) palloc0(sizeof(MySQLFdwExecState));

	fmstate->mysqlFdwOptions = mysql_get_options(foreignTableId, true);
	fmstate->conn = mysql_get_connection(server, user,
										 fmstate->mysqlFdwOptions);

	fmstate->query = strVal(list_nth(fdw_private, 0));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private, 1);

	n_params = list_length(fmstate->retrieved_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;
#if PG_VERSION_NUM >= 110000
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_DEFAULT_SIZES);
#else
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);
#endif

	if (mtstate->operation == CMD_UPDATE)
	{
		Form_pg_attribute attr;
#if PG_VERSION_NUM >= 140000
		Plan	   *subplan = outerPlanState(mtstate)->plan;
#else
		Plan	   *subplan = mtstate->mt_plans[subplan_index]->plan;
#endif

		Assert(subplan != NULL);

		attr = TupleDescAttr(RelationGetDescr(rel), 0);

		/* Find the rowid resjunk column in the subplan's result */
		fmstate->rowidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist,
														   NameStr(attr->attname));
		if (!AttributeNumberIsValid(fmstate->rowidAttno))
			elog(ERROR, "could not find junk row identifier column");
	}

	/* Set up for remaining transmittable parameters */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc);
		Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel),
											   attnum - 1);

		Assert(!attr->attisdropped);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);

	n_params = list_length(fmstate->retrieved_attrs);

	/* Initialize mysql statment */
	fmstate->stmt = mysql_stmt_init(fmstate->conn);
	if (!fmstate->stmt)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to initialize the MySQL query: \n%s",
						mysql_error(fmstate->conn))));

	/* Prepare mysql statment */
	if (mysql_stmt_prepare(fmstate->stmt, fmstate->query,
						   strlen(fmstate->query)) != 0)
		mysql_stmt_error_print(fmstate, "failed to prepare the MySQL query");

	resultRelInfo->ri_FdwState = fmstate;
}

/*
 * mysqlExecForeignInsert
 * 		Insert one row into a foreign table
 */
static TupleTableSlot *
mysqlExecForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState *fmstate;
	MYSQL_BIND *mysql_bind_buffer;
	ListCell   *lc;
	int			n_params;
	MemoryContext oldcontext;
	bool	   *isnull;
	char		sql_mode[255];
	Oid			foreignTableId = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	n_params = list_length(fmstate->retrieved_attrs);

	fmstate->mysqlFdwOptions = mysql_get_options(foreignTableId, true);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	mysql_bind_buffer = (MYSQL_BIND *) palloc0(sizeof(MYSQL_BIND) * n_params);
	isnull = (bool *) palloc0(sizeof(bool) * n_params);

	snprintf(sql_mode, sizeof(sql_mode), "SET sql_mode = '%s'",
			 fmstate->mysqlFdwOptions->sql_mode);
	if (mysql_query(fmstate->conn, sql_mode) != 0)
		mysql_error_print(fmstate->conn);

	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			type = TupleDescAttr(slot->tts_tupleDescriptor, attnum)->atttypid;
		Datum		value;

		value = slot_getattr(slot, attnum + 1, &isnull[attnum]);

		mysql_bind_sql_var(type, attnum, value, mysql_bind_buffer,
						   &isnull[attnum]);
	}

	/* Bind values */
	if (mysql_stmt_bind_param(fmstate->stmt, mysql_bind_buffer) != 0)
		mysql_stmt_error_print(fmstate, "failed to bind the MySQL query");

	/* Execute the query */
	if (mysql_stmt_execute(fmstate->stmt) != 0)
		mysql_stmt_error_print(fmstate, "failed to execute the MySQL query");

	MemoryContextSwitchTo(oldcontext);
	MemoryContextReset(fmstate->temp_cxt);
	return slot;
}

static TupleTableSlot *
mysqlExecForeignUpdate(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState *fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	MYSQL_BIND *mysql_bind_buffer;
	Oid			foreignTableId = RelationGetRelid(rel);
	bool		is_null = false;
	ListCell   *lc;
	int			bindnum = 0;
	Oid			typeoid;
	Datum		value;
	int			n_params;
	bool	   *isnull;
	Datum		new_value;
	HeapTuple	tuple;
	Form_pg_attribute attr;
	bool		found_row_id_col = false;

	n_params = list_length(fmstate->retrieved_attrs);

	mysql_bind_buffer = (MYSQL_BIND *) palloc0(sizeof(MYSQL_BIND) * n_params);
	isnull = (bool *) palloc0(sizeof(bool) * n_params);

	/* Bind the values */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc);
		Oid			type;

		/*
		 * The first attribute cannot be in the target list attribute.  Set the
		 * found_row_id_col to true once we find it so that we can fetch the
		 * value later.
		 */
		if (attnum == 1)
		{
			found_row_id_col = true;
			continue;
		}

		type = TupleDescAttr(slot->tts_tupleDescriptor, attnum - 1)->atttypid;
		value = slot_getattr(slot, attnum, (bool *) (&isnull[bindnum]));

		mysql_bind_sql_var(type, bindnum, value, mysql_bind_buffer,
						   &isnull[bindnum]);
		bindnum++;
	}

	/*
	 * Since we add a row identifier column in the target list always, so
	 * found_row_id_col flag should be true.
	 */
	if (!found_row_id_col)
		elog(ERROR, "missing row identifier column value in UPDATE");

	new_value = slot_getattr(slot, 1, &is_null);

	/*
	 * Get the row identifier column value that was passed up as a resjunk
	 * column and compare that value with the new value to identify if that
	 * value is changed.
	 */
	value = ExecGetJunkAttribute(planSlot, fmstate->rowidAttno, &is_null);

	tuple = SearchSysCache2(ATTNUM,
							ObjectIdGetDatum(foreignTableId),
							Int16GetDatum(1));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for attribute %d of relation %u",
			 1, foreignTableId);

	attr = (Form_pg_attribute) GETSTRUCT(tuple);
	typeoid = attr->atttypid;

	if (DatumGetPointer(new_value) != NULL && DatumGetPointer(value) != NULL)
	{
		Datum		n_value = new_value;
		Datum 		o_value = value;

		/* If the attribute type is varlena then need to detoast the datums. */
		if (attr->attlen == -1)
		{
			n_value = PointerGetDatum(PG_DETOAST_DATUM(new_value));
			o_value = PointerGetDatum(PG_DETOAST_DATUM(value));
		}

		if (!datumIsEqual(o_value, n_value, attr->attbyval, attr->attlen))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("row identifier column update is not supported")));

		/* Free memory if it's a copy made above */
		if (DatumGetPointer(n_value) != DatumGetPointer(new_value))
			pfree(DatumGetPointer(n_value));
		if (DatumGetPointer(o_value) != DatumGetPointer(value))
			pfree(DatumGetPointer(o_value));
	}
	else if (!(DatumGetPointer(new_value) == NULL &&
			   DatumGetPointer(value) == NULL))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("row identifier column update is not supported")));

	ReleaseSysCache(tuple);

	/* Bind qual */
	mysql_bind_sql_var(typeoid, bindnum, value, mysql_bind_buffer, &is_null);

	if (mysql_stmt_bind_param(fmstate->stmt, mysql_bind_buffer) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to bind the MySQL query: %s",
						mysql_error(fmstate->conn))));

	/* Execute the query */
	if (mysql_stmt_execute(fmstate->stmt) != 0)
		mysql_stmt_error_print(fmstate, "failed to execute the MySQL query");

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * mysqlAddForeignUpdateTargets
 * 		Add column(s) needed for update/delete on a foreign table, we are
 * 		using first column as row identification column, so we are adding
 * 		that into target list.
 */
#if PG_VERSION_NUM >= 140000
static void
mysqlAddForeignUpdateTargets(PlannerInfo *root,
							 Index rtindex,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
#else
static void
mysqlAddForeignUpdateTargets(Query *parsetree,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
#endif
{
	Var		   *var;
	const char *attrname;
#if PG_VERSION_NUM < 140000
	TargetEntry *tle;
#endif

	/*
	 * What we need is the rowid which is the first column
	 */
	Form_pg_attribute attr =
		TupleDescAttr(RelationGetDescr(target_relation), 0);

	/* Make a Var representing the desired value */
#if PG_VERSION_NUM >= 140000
	var = makeVar(rtindex,
#else
	var = makeVar(parsetree->resultRelation,
#endif
				  1,
				  attr->atttypid,
				  attr->atttypmod,
				  InvalidOid,
				  0);

	/* Get name of the row identifier column */
	attrname = NameStr(attr->attname);

#if PG_VERSION_NUM >= 140000
	/* Register it as a row-identity column needed by this target rel */
	add_row_identity_var(root, var, rtindex, attrname);
#else
	/* Wrap it in a TLE with the right name ... */
	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname), true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
#endif
}

/*
 * mysqlExecForeignDelete
 * 		Delete one row from a foreign table
 */
static TupleTableSlot *
mysqlExecForeignDelete(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState *fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	Relation	rel = resultRelInfo->ri_RelationDesc;
	MYSQL_BIND *mysql_bind_buffer;
	Oid			foreignTableId = RelationGetRelid(rel);
	bool		is_null = false;
	Oid			typeoid;
	Datum		value;

	mysql_bind_buffer = (MYSQL_BIND *) palloc(sizeof(MYSQL_BIND));

	/* Get the id that was passed up as a resjunk column */
	value = ExecGetJunkAttribute(planSlot, 1, &is_null);
	typeoid = get_atttype(foreignTableId, 1);

	/* Bind qual */
	mysql_bind_sql_var(typeoid, 0, value, mysql_bind_buffer, &is_null);

	if (mysql_stmt_bind_param(fmstate->stmt, mysql_bind_buffer) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to execute the MySQL query: %s",
						mysql_error(fmstate->conn))));

	/* Execute the query */
	if (mysql_stmt_execute(fmstate->stmt) != 0)
		mysql_stmt_error_print(fmstate, "failed to execute the MySQL query");

	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * mysqlEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
mysqlEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
	MySQLFdwExecState *festate = resultRelInfo->ri_FdwState;

	if (festate && festate->stmt)
	{
		mysql_stmt_close(festate->stmt);
		festate->stmt = NULL;
	}
}

/*
 * mysqlImportForeignSchema
 * 		Import a foreign schema (9.5+)
 */
static List *
mysqlImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	List	   *commands = NIL;
	bool		import_default = false;
	bool		import_not_null = true;
	bool		import_enum_as_text = false;
	ForeignServer *server;
	UserMapping *user;
	mysql_opt  *options;
	MYSQL	   *conn;
	StringInfoData buf;
	MYSQL_RES  *volatile res = NULL;
	MYSQL_ROW	row;
	ListCell   *lc;

	/* Parse statement options */
	foreach(lc, stmt->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "import_default") == 0)
			import_default = defGetBoolean(def);
		else if (strcmp(def->defname, "import_not_null") == 0)
			import_not_null = defGetBoolean(def);
		else if (strcmp(def->defname, "import_enum_as_text") == 0)
			import_enum_as_text = defGetBoolean(def);
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname)));
	}

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	server = GetForeignServer(serverOid);
	user = GetUserMapping(GetUserId(), server->serverid);
	options = mysql_get_options(serverOid, false);
	conn = mysql_get_connection(server, user, options);

	/* Create workspace for strings */
	initStringInfo(&buf);

	/* Check that the schema really exists */
	appendStringInfo(&buf,
					 "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s'",
					 stmt->remote_schema);

	if (mysql_query(conn, buf.data) != 0)
		mysql_error_print(conn);

	res = mysql_store_result(conn);
	if (!res || mysql_num_rows(res) < 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
				 errmsg("schema \"%s\" is not present on foreign server \"%s\"",
						stmt->remote_schema, server->servername)));

	mysql_free_result(res);
	res = NULL;
	resetStringInfo(&buf);

	/*
	 * Fetch all table data from this schema, possibly restricted by EXCEPT or
	 * LIMIT TO.
	 */
	appendStringInfo(&buf,
					 " SELECT"
					 "  t.TABLE_NAME,"
					 "  c.COLUMN_NAME,"
					 "  CASE"
					 "    WHEN c.DATA_TYPE = 'enum' THEN LOWER(CONCAT(t.TABLE_NAME, '_', c.COLUMN_NAME, '_t'))"
					 "    WHEN c.DATA_TYPE = 'tinyint' THEN 'smallint'"
					 "    WHEN c.DATA_TYPE = 'mediumint' THEN 'integer'"
					 "    WHEN c.DATA_TYPE = 'tinyint unsigned' THEN 'smallint'"
					 "    WHEN c.DATA_TYPE = 'smallint unsigned' THEN 'integer'"
					 "    WHEN c.DATA_TYPE = 'mediumint unsigned' THEN 'integer'"
					 "    WHEN c.DATA_TYPE = 'int unsigned' THEN 'bigint'"
					 "    WHEN c.DATA_TYPE = 'bigint unsigned' THEN 'numeric(20)'"
					 "    WHEN c.DATA_TYPE = 'double' THEN 'double precision'"
					 "    WHEN c.DATA_TYPE = 'float' THEN 'real'"
					 "    WHEN c.DATA_TYPE = 'datetime' THEN 'timestamp'"
					 "    WHEN c.DATA_TYPE = 'longtext' THEN 'text'"
					 "    WHEN c.DATA_TYPE = 'mediumtext' THEN 'text'"
					 "    WHEN c.DATA_TYPE = 'tinytext' THEN 'text'"
					 "    WHEN c.DATA_TYPE = 'blob' THEN 'bytea'"
					 "    WHEN c.DATA_TYPE = 'mediumblob' THEN 'bytea'"
					 "    WHEN c.DATA_TYPE = 'longblob' THEN 'bytea'"
					 "    WHEN c.DATA_TYPE = 'binary' THEN 'bytea'"
					 "    WHEN c.DATA_TYPE = 'varbinary' THEN 'bytea'"
					 "    ELSE c.DATA_TYPE"
					 "  END,"
					 "  c.COLUMN_TYPE,"
					 "  IF(c.IS_NULLABLE = 'NO', 't', 'f'),"
					 "  c.COLUMN_DEFAULT"
					 " FROM"
					 "  information_schema.TABLES AS t"
					 " JOIN"
					 "  information_schema.COLUMNS AS c"
					 " ON"
					 "  t.TABLE_CATALOG <=> c.TABLE_CATALOG AND t.TABLE_SCHEMA <=> c.TABLE_SCHEMA AND t.TABLE_NAME <=> c.TABLE_NAME"
					 " WHERE"
					 "  t.TABLE_SCHEMA = '%s'",
					 stmt->remote_schema);

	/* Apply restrictions for LIMIT TO and EXCEPT */
	if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
		stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
	{
		bool		first_item = true;

		appendStringInfoString(&buf, " AND t.TABLE_NAME ");
		if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
			appendStringInfoString(&buf, "NOT ");
		appendStringInfoString(&buf, "IN (");

		/* Append list of table names within IN clause */
		foreach(lc, stmt->table_list)
		{
			RangeVar   *rv = (RangeVar *) lfirst(lc);

			if (first_item)
				first_item = false;
			else
				appendStringInfoString(&buf, ", ");

			appendStringInfo(&buf, "'%s'", rv->relname);
		}
		appendStringInfoChar(&buf, ')');
	}

	/* Append ORDER BY at the end of query to ensure output ordering */
	appendStringInfo(&buf, " ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION");

	/* Fetch the data */
	if (mysql_query(conn, buf.data) != 0)
		mysql_error_print(conn);

	res = mysql_store_result(conn);
	row = mysql_fetch_row(res);
	while (row)
	{
		char	   *tablename = row[0];
		bool		first_item = true;
		bool		has_set = false;

		resetStringInfo(&buf);
		appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n",
						 quote_identifier(tablename));

		/* Scan all rows for this table */
		do
		{
			char	   *attname;
			char	   *typename;
			char	   *typedfn;
			char	   *attnotnull;
			char	   *attdefault;

			/*
			 * If the table has no columns, we'll see nulls here. Also, if we
			 * have already discovered this table has a SET type column, we
			 * better skip the rest of the checking.
			 */
			if (row[1] == NULL || has_set)
				continue;

			attname = row[1];
			typename = row[2];

			if (strcmp(typename, "char") == 0 || strcmp(typename, "varchar") == 0)
				typename = row[3];

			typedfn = row[3];
			attnotnull = row[4];
			attdefault = row[5] == NULL ? (char *) NULL : row[5];

			if (strncmp(typedfn, "enum(", 5) == 0)
			{
				/*
				 * If import_enum_as_text is set, then map MySQL enum type to
				 * text while import, else emit a warning to create mapping
				 * TYPE.
				 */
				if (import_enum_as_text)
					typename = "text";
				else
					ereport(NOTICE,
							(errmsg("error while generating the table definition"),
							 errhint("If you encounter an error, you may need to execute the following first:\nDO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typname = '%s') THEN CREATE TYPE %s AS %s; END IF; END$$;\n",
									 typename, typename, typedfn)));
			}

			/*
			 * PostgreSQL does not have an equivalent data type to map with
			 * SET, so skip the table definitions for the ones having SET type
			 * column.
			 */
			if (strncmp(typedfn, "set", 3) == 0)
			{
				ereport(WARNING,
						(errmsg("skipping import for relation \"%s\"", quote_identifier(tablename)),
						 errdetail("MySQL SET columns are not supported.")));

				has_set = true;
				continue;
			}

			if (first_item)
				first_item = false;
			else
				appendStringInfoString(&buf, ",\n");

			/* Print column name and type */
			appendStringInfo(&buf, "  %s %s", quote_identifier(attname),
							 typename);

			/* Add DEFAULT if needed */
			if (import_default && attdefault != NULL)
				appendStringInfo(&buf, " DEFAULT %s", attdefault);

			/* Add NOT NULL if needed */
			if (import_not_null && attnotnull[0] == 't')
				appendStringInfoString(&buf, " NOT NULL");
		}
		while ((row = mysql_fetch_row(res)) &&
			   (strcmp(row[0], tablename) == 0));

		/*
		 * As explained above, skip importing relations that have SET type
		 * column.
		 */
		if (has_set)
			continue;

		/*
		 * Add server name and table-level options.  We specify remote
		 * database and table name as options (the latter to ensure that
		 * renaming the foreign table doesn't break the association).
		 */
		appendStringInfo(&buf,
						 "\n) SERVER %s OPTIONS (dbname '%s', table_name '%s');\n",
						 quote_identifier(server->servername),
						 stmt->remote_schema,
						 tablename);

		commands = lappend(commands, pstrdup(buf.data));
	}

	/* Clean up */
	mysql_free_result(res);
	res = NULL;
	resetStringInfo(&buf);

	mysql_release_connection(conn);

	return commands;
}

#if PG_VERSION_NUM >= 110000
/*
 * mysqlBeginForeignInsert
 * 		Prepare for an insert operation triggered by partition routing
 * 		or COPY FROM.
 *
 * This is not yet supported, so raise an error.
 */
static void
mysqlBeginForeignInsert(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mysql_fdw")));
}

/*
 * mysqlEndForeignInsert
 * 		BeginForeignInsert() is not yet implemented, hence we do not
 * 		have anything to cleanup as of now. We throw an error here just
 * 		to make sure when we do that we do not forget to cleanup
 * 		resources.
 */
static void
mysqlEndForeignInsert(EState *estate, ResultRelInfo *resultRelInfo)
{
	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("COPY and foreign partition routing not supported in mysql_fdw")));
}
#endif

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node,
					 List *fdw_exprs,
					 int numParams,
					 FmgrInfo **param_flinfo,
					 List **param_exprs,
					 const char ***param_values,
					 Oid **param_types)
{
	int			i;
	ListCell   *lc;

	Assert(numParams > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * numParams);

	*param_types = (Oid *) palloc0(sizeof(Oid) * numParams);

	i = 0;
	foreach(lc, fdw_exprs)
	{
		Node	   *param_expr = (Node *) lfirst(lc);
		Oid			typefnoid;
		bool		isvarlena;

		(*param_types)[i] = exprType(param_expr);

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require postgres_fdw to know more than is desirable
	 * about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(numParams * sizeof(char *));
}

/*
 * Construct array of query parameter values in text format.
 */
static void
process_query_params(ExprContext *econtext,
					 FmgrInfo *param_flinfo,
					 List *param_exprs,
					 const char **param_values,
					 MYSQL_BIND **mysql_bind_buf,
					 Oid *param_types)
{
	int			i;
	ListCell   *lc;

	i = 0;
	foreach(lc, param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull);
		mysql_bind_sql_var(param_types[i], i, expr_value, *mysql_bind_buf,
						   &isNull);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (isNull)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);
		i++;
	}
}

/*
 * Process the query params and bind the same with the statement, if any.
 * Also, execute the statement. If fetching the var size column then bind
 * those again to allocate field->max_length memory.
 */
static void
bind_stmt_params_and_exec(ForeignScanState *node)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	int			numParams = festate->numParams;
	const char **values = festate->param_values;
	MYSQL_BIND *mysql_bind_buffer = NULL;
	ListCell   *lc;
	TupleDesc	tupleDescriptor = festate->attinmeta->tupdesc;
	int			atindex = 0;
	MemoryContext oldcontext;

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (numParams > 0)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

		mysql_bind_buffer = (MYSQL_BIND *) palloc0(sizeof(MYSQL_BIND) * numParams);

		process_query_params(econtext,
							 festate->param_flinfo,
							 festate->param_exprs,
							 values,
							 &mysql_bind_buffer,
							 festate->param_types);

		mysql_stmt_bind_param(festate->stmt, mysql_bind_buffer);

		MemoryContextSwitchTo(oldcontext);
	}

	/*
	 * Finally, execute the query. The result will be placed in the array we
	 * already bind.
	 */
	if (mysql_stmt_execute(festate->stmt) != 0)
		mysql_stmt_error_print(festate, "failed to execute the MySQL query");

	/* Mark the query as executed */
	festate->query_executed = true;

	if (!festate->has_var_size_col)
		return;

	/* Bind the result columns in long-lived memory context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

	if (mysql_stmt_store_result(festate->stmt) != 0)
		mysql_stmt_error_print(festate, "failed to store the result");

	/* Bind only var size columns as per field->max_length */
	foreach(lc, festate->retrieved_attrs)
	{
		int			attnum = lfirst_int(lc) - 1;
		Oid			pgtype = TupleDescAttr(tupleDescriptor, attnum)->atttypid;
		int32		pgtypmod = TupleDescAttr(tupleDescriptor, attnum)->atttypmod;

		if (TupleDescAttr(tupleDescriptor, attnum)->attisdropped)
			continue;

		if (pgtype != TEXTOID)
		{
			atindex++;
			continue;
		}

		festate->table->column[atindex].mysql_bind = &festate->table->mysql_bind[atindex];

		mysql_bind_result(pgtype, pgtypmod,
						  &festate->table->mysql_fields[atindex],
						  &festate->table->column[atindex]);
		atindex++;
	}

	/* Bind the results pointers for the prepare statements */
	if (mysql_stmt_bind_result(festate->stmt, festate->table->mysql_bind) != 0)
		mysql_stmt_error_print(festate, "failed to bind the MySQL query");

	MemoryContextSwitchTo(oldcontext);
}

Datum
mysql_fdw_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CODE_VERSION);
}

static void
mysql_error_print(MYSQL *conn)
{
	switch (mysql_errno(conn))
	{
		case CR_NO_ERROR:
			/* Should not happen, though give some message */
			elog(ERROR, "unexpected error code");
			break;
		case CR_OUT_OF_MEMORY:
		case CR_SERVER_GONE_ERROR:
		case CR_SERVER_LOST:
		case CR_UNKNOWN_ERROR:
			mysql_release_connection(conn);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to execute the MySQL query: \n%s",
							mysql_error(conn))));
			break;
		case CR_COMMANDS_OUT_OF_SYNC:
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to execute the MySQL query: \n%s",
							mysql_error(conn))));
	}
}

static void
mysql_stmt_error_print(MySQLFdwExecState *festate, const char *msg)
{
	switch (mysql_stmt_errno(festate->stmt))
	{
		case CR_NO_ERROR:
			/* Should not happen, though give some message */
			elog(ERROR, "unexpected error code");
			break;
		case CR_OUT_OF_MEMORY:
		case CR_SERVER_GONE_ERROR:
		case CR_SERVER_LOST:
		case CR_UNKNOWN_ERROR:
			mysql_release_connection(festate->conn);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("%s: \n%s", msg, mysql_error(festate->conn))));
			break;
		case CR_COMMANDS_OUT_OF_SYNC:
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("%s: \n%s", msg, mysql_error(festate->conn))));
			break;
	}
}

/*
 * getUpdateTargetAttrs
 * 		Returns the list of attribute numbers of the columns being updated.
 */
static List *
getUpdateTargetAttrs(RangeTblEntry *rte)
{
	List	   *targetAttrs = NIL;

	Bitmapset  *tmpset = bms_copy(rte->updatedCols);
	AttrNumber	col;

	while ((col = bms_first_member(tmpset)) >= 0)
	{
		col += FirstLowInvalidHeapAttributeNumber;
		if (col <= InvalidAttrNumber)	/* shouldn't happen */
			elog(ERROR, "system-column update is not supported");

		/* We also disallow updates to the first column */
		if (col == 1)
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("row identifier column update is not supported")));

		targetAttrs = lappend_int(targetAttrs, col);
	}

	return targetAttrs;
}

/*
 * mysqlGetForeignJoinPaths
 *		Add possible ForeignPath to joinrel, if join is safe to push down.
 */
static void
mysqlGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel,
						 RelOptInfo *outerrel, RelOptInfo *innerrel,
						 JoinType jointype, JoinPathExtraData *extra)
{
	MySQLFdwRelationInfo *fpinfo;
	ForeignPath *joinpath;
	Cost		startup_cost;
	Cost		total_cost;
	Path	   *epq_path = NULL; /* Path to create plan to be executed when
								  * EvalPlanQual gets triggered. */

	/*
	 * Skip if this join combination has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/*
	 * Create unfinished MySQLFdwRelationInfo entry which is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe.  Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	joinrel->fdw_private = fpinfo;
	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;

	/*
	 * In case there is a possibility that EvalPlanQual will be executed, we
	 * should be able to reconstruct the row, from base relations applying all
	 * the conditions.  We create a local plan from a suitable local path
	 * available in the path list.  In case such a path doesn't exist, we can
	 * not push the join to the foreign server since we won't be able to
	 * reconstruct the row for EvalPlanQual().  Find an alternative local path
	 * before we add ForeignPath, lest the new path would kick possibly the
	 * only local path.  Do this before calling mysql_foreign_join_ok(), since
	 * that function updates fpinfo and marks it as pushable if the join is
	 * found to be pushable.
	 */
	if (root->parse->commandType == CMD_DELETE ||
		root->parse->commandType == CMD_UPDATE ||
		root->rowMarks)
	{
		epq_path = GetExistingLocalJoinPath(joinrel);
		if (!epq_path)
		{
			elog(DEBUG3, "could not push down foreign join because a local path suitable for EPQ checks was not found");
			return;
		}
	}
	else
		epq_path = NULL;

	if (!mysql_foreign_join_ok(root, joinrel, jointype, outerrel, innerrel,
							   extra))
	{
		/* Free path required for EPQ if we copied one; we don't need it now */
		if (epq_path)
			pfree(epq_path);
		return;
	}

	/* TODO: Put accurate estimates here */
	startup_cost = 15.0;
	total_cost = 20 + startup_cost;

	/*
	 * Create a new join path and add it to the joinrel which represents a
	 * join between foreign tables.
	 */
#if PG_VERSION_NUM >= 120000
	joinpath = create_foreign_join_path(root,
									   joinrel,
									   NULL,	/* default pathtarget */
									   joinrel->rows,
									   startup_cost,
									   total_cost,
									   NIL,		/* no pathkeys */
									   joinrel->lateral_relids,
									   epq_path,
									   NIL);	/* no fdw_private */
#else
	joinpath = create_foreignscan_path(root,
									   joinrel,
									   NULL,	/* default pathtarget */
									   joinrel->rows,
									   startup_cost,
									   total_cost,
									   NIL, 	/* no pathkeys */
									   joinrel->lateral_relids,
									   epq_path,
									   NIL);	/* no fdw_private */
#endif      /* PG_VERSION_NUM >= 120000 */

	/* Add generated path into joinrel by add_path(). */
	add_path(joinrel, (Path *) joinpath);

	/* XXX Consider pathkeys for the join relation */

	/* XXX Consider parameterized paths for the join relation */
}

/*
 * mysql_foreign_join_ok
 * 		Assess whether the join between inner and outer relations can be
 * 		pushed down to the foreign server.
 *
 * As a side effect, save information we obtain in this function to
 * MySQLFdwRelationInfo passed in.
 */
static bool
mysql_foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel,
					  JoinType jointype, RelOptInfo *outerrel,
					  RelOptInfo *innerrel, JoinPathExtraData *extra)
{
	MySQLFdwRelationInfo *fpinfo;
	MySQLFdwRelationInfo *fpinfo_o;
	MySQLFdwRelationInfo *fpinfo_i;
	ListCell   *lc;
	List	   *joinclauses;

	/*
	 * We support pushing down INNER, LEFT and RIGHT joins.
	 * Constructing queries representing SEMI and ANTI joins is hard, hence
	 * not considered right now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT)
		return false;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join cannot be pushed down.
	 */
	fpinfo = (MySQLFdwRelationInfo *) joinrel->fdw_private;
	fpinfo_o = (MySQLFdwRelationInfo *) outerrel->fdw_private;
	fpinfo_i = (MySQLFdwRelationInfo *) innerrel->fdw_private;
	if (!fpinfo_o || !fpinfo_o->pushdown_safe ||
		!fpinfo_i || !fpinfo_i->pushdown_safe)
		return false;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations.  Hence the join can
	 * not be pushed down.
	 */
	if (fpinfo_o->local_conds || fpinfo_i->local_conds)
		return false;

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach(lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool		is_remote_clause = mysql_is_foreign_expr(root, joinrel,
															 rinfo->clause,
															 true);

		if (IS_OUTER_JOIN(jointype) &&
			!RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
			{
				/*
				 * Unlike postgres_fdw, don't append the join clauses to
				 * remote_conds, instead keep the join clauses separate.
				 * Currently, we are providing limited operator pushability
				 * support for join pushdown, hence we keep those clauses
				 * separate to avoid INNER JOIN not getting pushdown if any
				 * of the WHERE clause is not shippable as per join pushdown
				 * shippability.
				 */
				if (jointype == JOIN_INNER)
					joinclauses = lappend(joinclauses, rinfo);
				else
					fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			}
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * mysqlDeparseExplicitTargetList() isn't smart enough to handle anything
	 * other than a Var.  In particular, if there's some PlaceHolderVar that
	 * would need to be evaluated within this join tree (because there's an
	 * upper reference to a quantity that may go to NULL as a result of an
	 * outer join), then we can't try to push the join down because we'll fail
	 * when we get to mysqlDeparseExplicitTargetList().  However, a
	 * PlaceHolderVar that needs to be evaluated *at the top* of this join tree
	 * is OK, because we can do that locally after fetching the results from
	 * the remote side.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids		relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
			joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation.  This
	 * avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses.  For an OUTER join, the clauses from the outer
	 * side are added to remote_conds since those can be evaluated after the
	 * join is evaluated.  The clauses from inner side are added to the
	 * joinclauses, since they need to evaluated while constructing the join.
	 *
	 * The joining sides cannot have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_i->remote_conds);
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_o->remote_conds);
			break;

		case JOIN_LEFT:
			/* Check that clauses from the inner side are pushable or not. */
			foreach(lc, fpinfo_i->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (!mysql_is_foreign_expr(root, joinrel, ri->clause, true))
					return false;
			}

			fpinfo->joinclauses = mysql_list_concat(fpinfo->joinclauses,
													fpinfo_i->remote_conds);
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_o->remote_conds);
			break;

		case JOIN_RIGHT:
			/* Check that clauses from the outer side are pushable or not. */
			foreach(lc, fpinfo_o->remote_conds)
			{
				RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

				if (!mysql_is_foreign_expr(root, joinrel, ri->clause, true))
					return false;
			}

			fpinfo->joinclauses = mysql_list_concat(fpinfo->joinclauses,
													fpinfo_o->remote_conds);
			fpinfo->remote_conds = mysql_list_concat(fpinfo->remote_conds,
													 fpinfo_i->remote_conds);
			break;

		default:
			/* Should not happen, we have just check this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "(%s) %s JOIN (%s)",
					 fpinfo_o->relation_name->data,
					 mysql_get_jointype_name(fpinfo->jointype),
					 fpinfo_i->relation_name->data);

	return true;
}

/*
 * mysqlRecheckForeignScan
 *		Execute a local join execution plan for a foreign join.
 */
static bool
mysqlRecheckForeignScan(ForeignScanState *node, TupleTableSlot *slot)
{
	Index		scanrelid = ((Scan *) node->ss.ps.plan)->scanrelid;
	PlanState  *outerPlan = outerPlanState(node);
	TupleTableSlot *result;

	/* For base foreign relations, it suffices to set fdw_recheck_quals */
	if (scanrelid > 0)
		return true;

	Assert(outerPlan != NULL);

	/* Execute a local join execution plan */
	result = ExecProcNode(outerPlan);
	if (TupIsNull(result))
		return false;

	/* Store result in the given slot */
	ExecCopySlot(slot, result);

	return true;
}

/*
 * mysql_adjust_whole_row_ref
 * 		If the given list of Var nodes has whole-row reference, add Var
 * 		nodes corresponding to all the attributes of the corresponding
 * 		base relation.
 *
 * The function also returns an array of lists of var nodes.  The array is
 * indexed by the RTI and entry there contains the list of Var nodes which
 * make up the whole-row reference for corresponding base relation.
 * The relations not covered by given join and the relations which do not
 * have whole-row references will have NIL entries.
 *
 * If there are no whole-row references in the given list, the given list is
 * returned unmodified and the other list is NIL.
 */
static List *
mysql_adjust_whole_row_ref(PlannerInfo *root, List *scan_var_list,
						   List **whole_row_lists, Bitmapset *relids)
{
	ListCell   *lc;
	bool		has_whole_row = false;
	List	  **wr_list_array = NULL;
	int			cnt_rt;
	List	   *wr_scan_var_list = NIL;

	*whole_row_lists = NIL;

	/* Check if there exists at least one whole row reference. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		if (var->varattno == 0)
		{
			has_whole_row = true;
			break;
		}
	}

	if (!has_whole_row)
		return scan_var_list;

	/*
	 * Allocate large enough memory to hold whole-row Var lists for all the
	 * relations.  This array will then be converted into a list of lists.
	 * Since all the base relations are marked by range table index, it's easy
	 * to keep track of the ones whose whole-row references have been taken
	 * care of.
	 */
	wr_list_array = (List **) palloc0(sizeof(List *) *
									  list_length(root->parse->rtable));

	/* Adjust the whole-row references as described in the prologue. */
	foreach(lc, scan_var_list)
	{
		Var		   *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		if (var->varattno == 0 && !wr_list_array[var->varno - 1])
		{
			List	   *wr_var_list;
			List	   *retrieved_attrs;
			RangeTblEntry *rte = rt_fetch(var->varno, root->parse->rtable);
			Bitmapset  *attrs_used;

			Assert(OidIsValid(rte->relid));

			/*
			 * Get list of Var nodes for all undropped attributes of the base
			 * relation.
			 */
			attrs_used = bms_make_singleton(0 -
											FirstLowInvalidHeapAttributeNumber);

			/*
			 * If the whole-row reference falls on the nullable side of the
			 * outer join and that side is null in a given result row, the
			 * whole row reference should be set to NULL.  In this case, all
			 * the columns of that relation will be NULL, but that does not
			 * help since those columns can be genuinely NULL in a row.
			 */
			wr_var_list =
				mysql_build_scan_list_for_baserel(rte->relid, var->varno,
												  attrs_used,
												  &retrieved_attrs);
			wr_list_array[var->varno - 1] = wr_var_list;
			wr_scan_var_list = list_concat_unique(wr_scan_var_list,
												  wr_var_list);
			bms_free(attrs_used);
			list_free(retrieved_attrs);
		}
		else
			wr_scan_var_list = list_append_unique(wr_scan_var_list, var);
	}

	/*
	 * Collect the required Var node lists into a list of lists ordered by the
	 * base relations' range table indexes.
	 */
	cnt_rt = -1;
	while ((cnt_rt = bms_next_member(relids, cnt_rt)) >= 0)
		*whole_row_lists = lappend(*whole_row_lists, wr_list_array[cnt_rt - 1]);

	pfree(wr_list_array);
	return wr_scan_var_list;
}

/*
 * mysql_build_scan_list_for_baserel
 * 		Build list of nodes corresponding to the attributes requested for
 * 		given base relation.
 *
 * The list contains Var nodes corresponding to the attributes specified in
 * attrs_used.  If whole-row reference is required, the functions adds Var
 * nodes corresponding to all the attributes in the relation.
 */
static List *
mysql_build_scan_list_for_baserel(Oid relid, Index varno,
								  Bitmapset *attrs_used,
								  List **retrieved_attrs)
{
	int			attno;
	List	   *tlist = NIL;
	Node	   *node;
	bool		wholerow_requested = false;
	Relation	relation;
	TupleDesc	tupdesc;

	Assert(OidIsValid(relid));

	*retrieved_attrs = NIL;

	/* Planner must have taken a lock, so request no lock here */
#if PG_VERSION_NUM < 130000
	relation = heap_open(relid, NoLock);
#else
	relation = table_open(relid, NoLock);
#endif

	tupdesc = RelationGetDescr(relation);

	/* Is whole-row reference requested? */
	wholerow_requested = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
									   attrs_used);

	/* Handle user defined attributes. */
	for (attno = 1; attno <= tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		/*
		 * For a required attribute create a Var node and add corresponding
		 * attribute number to the retrieved_attrs list.
		 */
		if (wholerow_requested ||
			bms_is_member(attno - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			node = (Node *) makeVar(varno, attno, attr->atttypid,
									attr->atttypmod, attr->attcollation, 0);
			tlist = lappend(tlist, node);

			*retrieved_attrs = lappend_int(*retrieved_attrs, attno);
		}
	}

#if PG_VERSION_NUM < 130000
	heap_close(relation, NoLock);
#else
	table_close(relation, NoLock);
#endif

	return tlist;
}

/*
 * mysql_build_whole_row_constr_info
 *		Calculate and save the information required to construct whole row
 *		references of base foreign relations involved in the pushed down join.
 *
 * tupdesc is the tuple descriptor describing the result returned by the
 * ForeignScan node.  It is expected to be same as
 * ForeignScanState::ss::ss_ScanTupleSlot, which is constructed using
 * fdw_scan_tlist.
 *
 * relids is the the set of relations participating in the pushed down join.
 *
 * max_relid is the maximum number of relation index expected.
 *
 * whole_row_lists is the list of Var node lists constituting the whole-row
 * reference for base relations in the relids in the same order.
 *
 * scan_tlist is the targetlist representing the result fetched from the
 * foreign server.
 *
 * fdw_scan_tlist is the targetlist representing the result returned by the
 * ForeignScan node.
 */
static void
mysql_build_whole_row_constr_info(MySQLFdwExecState *festate,
								  TupleDesc tupdesc, Bitmapset *relids,
								  int max_relid, List *whole_row_lists,
								  List *scan_tlist, List *fdw_scan_tlist)
{
	int			cnt_rt;
	int			cnt_vl;
	int			cnt_attr;
	ListCell   *lc;
	int		   *fs_attr_pos = NULL;
	MySQLWRState **mysqlwrstates = NULL;
	int			fs_num_atts;

	/*
	 * Allocate memory to hold whole-row reference state for each relation.
	 * Indexing by the range table index is faster than maintaining an
	 * associative map.
	 */
	mysqlwrstates = (MySQLWRState **) palloc0(sizeof(MySQLWRState *) * max_relid);

	/*
	 * Set the whole-row reference state for the relations whose whole-row
	 * reference needs to be constructed.
	 */
	cnt_rt = -1;
	cnt_vl = 0;
	while ((cnt_rt = bms_next_member(relids, cnt_rt)) >= 0)
	{
		MySQLWRState *wr_state = (MySQLWRState *) palloc0(sizeof(MySQLWRState));
		List	   *var_list = list_nth(whole_row_lists, cnt_vl++);
		int			natts;

		/* Skip the relations without whole-row references. */
		if (list_length(var_list) <= 0)
			continue;

		natts = list_length(var_list);
		wr_state->attr_pos = (int *) palloc(sizeof(int) * natts);

		/*
		 * Create a map of attributes required for whole-row reference to
		 * their positions in the result fetched from the foreign server.
		 */
		cnt_attr = 0;
		foreach(lc, var_list)
		{
			Var		   *var = lfirst(lc);
			TargetEntry *tle_sl;

			Assert(IsA(var, Var) &&var->varno == cnt_rt);

			tle_sl = tlist_member((Expr *) var, scan_tlist);

			Assert(tle_sl);

			wr_state->attr_pos[cnt_attr++] = tle_sl->resno - 1;
		}
		Assert(natts == cnt_attr);

		/* Build rest of the state */
		wr_state->tupdesc = ExecTypeFromExprList(var_list);
		Assert(natts == wr_state->tupdesc->natts);
		wr_state->values = (Datum *) palloc(sizeof(Datum) * natts);
		wr_state->nulls = (bool *) palloc(sizeof(bool) * natts);
		BlessTupleDesc(wr_state->tupdesc);
		mysqlwrstates[cnt_rt - 1] = wr_state;
	}

	/*
	 * Construct the array mapping columns in the ForeignScan node output to
	 * their positions in the result fetched from the foreign server.  Positive
	 * values indicate the locations in the result and negative values
	 * indicate the range table indexes of the base table whose whole-row
	 * reference values are requested in that place.
	 */
	fs_num_atts = list_length(fdw_scan_tlist);
	fs_attr_pos = (int *) palloc(sizeof(int) * fs_num_atts);
	cnt_attr = 0;
	foreach(lc, fdw_scan_tlist)
	{
		TargetEntry *tle_fsl = lfirst(lc);
		Var		   *var = (Var *) tle_fsl->expr;

		Assert(IsA(var, Var));
		if (var->varattno == 0)
			fs_attr_pos[cnt_attr] = -var->varno;
		else
		{
			TargetEntry *tle_sl = tlist_member((Expr *) var, scan_tlist);

			Assert(tle_sl);
			fs_attr_pos[cnt_attr] = tle_sl->resno - 1;
		}
		cnt_attr++;
	}

	/*
	 * The tuple descriptor passed in should have same number of attributes as
	 * the entries in fdw_scan_tlist.
	 */
	Assert(fs_num_atts == tupdesc->natts);

	festate->mysqlwrstates = mysqlwrstates;
	festate->wr_attrs_pos = fs_attr_pos;
	festate->wr_tupdesc = tupdesc;
	festate->wr_values = (Datum *) palloc(sizeof(Datum) * tupdesc->natts);
	festate->wr_nulls = (bool *) palloc(sizeof(bool) * tupdesc->natts);

	return;
}

/*
 * mysql_get_tuple_with_whole_row
 *		Construct the result row with whole-row references.
 */
static HeapTuple
mysql_get_tuple_with_whole_row(MySQLFdwExecState *festate, Datum *values,
							   bool *nulls)
{
	TupleDesc	tupdesc = festate->wr_tupdesc;
	Datum	   *wr_values = festate->wr_values;
	bool	   *wr_nulls = festate->wr_nulls;
	int			cnt_attr;
	HeapTuple	tuple = NULL;

	for (cnt_attr = 0; cnt_attr < tupdesc->natts; cnt_attr++)
	{
		int			attr_pos = festate->wr_attrs_pos[cnt_attr];

		if (attr_pos >= 0)
		{
			wr_values[cnt_attr] = values[attr_pos];
			wr_nulls[cnt_attr] = nulls[attr_pos];
		}
		else
		{
			/*
			 * The RTI of relation whose whole row reference is to be
			 * constructed is stored as -ve attr_pos.
			 */
			MySQLWRState *wr_state = festate->mysqlwrstates[-attr_pos - 1];

			wr_nulls[cnt_attr] = nulls[wr_state->wr_null_ind_pos];
			if (!wr_nulls[cnt_attr])
			{
				HeapTuple	wr_tuple = mysql_form_whole_row(wr_state,
															values,
															nulls);

				wr_values[cnt_attr] = HeapTupleGetDatum(wr_tuple);
			}
		}
	}

	tuple = heap_form_tuple(tupdesc, wr_values, wr_nulls);
	return tuple;
}

/*
 * mysql_form_whole_row
 * 		The function constructs whole-row reference for a base relation
 * 		with the information given in wr_state.
 *
 * wr_state contains the information about which attributes from values and
 * nulls are to be used and in which order to construct the whole-row
 * reference.
 */
static HeapTuple
mysql_form_whole_row(MySQLWRState *wr_state, Datum *values, bool *nulls)
{
	int			cnt_attr;

	for (cnt_attr = 0; cnt_attr < wr_state->tupdesc->natts; cnt_attr++)
	{
		int			attr_pos = wr_state->attr_pos[cnt_attr];

		wr_state->values[cnt_attr] = values[attr_pos];
		wr_state->nulls[cnt_attr] = nulls[attr_pos];
	}
	return heap_form_tuple(wr_state->tupdesc, wr_state->values,
						   wr_state->nulls);
}

/*
 * mysql_foreign_grouping_ok
 * 		Assess whether the aggregation, grouping and having operations can
 * 		be pushed down to the foreign server.  As a side effect, save
 * 		information we obtain in this function to MySQLFdwRelationInfo of
 * 		the input relation.
 */
#if PG_VERSION_NUM >= 110000
static bool
mysql_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel,
						  Node *havingQual)
#elif PG_VERSION_NUM >= 100000
static bool
mysql_foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel)
#endif
{
	Query	   *query = root->parse;
#if PG_VERSION_NUM >= 110000
	PathTarget *grouping_target =  grouped_rel->reltarget;
#elif PG_VERSION_NUM >= 100000
	PathTarget *grouping_target = root->upper_targets[UPPERREL_GROUP_AGG];
#endif
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) grouped_rel->fdw_private;
	MySQLFdwRelationInfo *ofpinfo;
	ListCell   *lc;
	int			i;
	List	   *tlist = NIL;

	/* Grouping Sets are not pushable */
	if (query->groupingSets)
		return false;

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (MySQLFdwRelationInfo *) fpinfo->outerrel->fdw_private;

	/*
	 * If underneath input relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Evaluate grouping targets and check whether they are safe to push down
	 * to the foreign side.  All GROUP BY expressions will be part of the
	 * grouping target and thus there is no need to evaluate it separately.
	 * While doing so, add required expressions into target list which can
	 * then be used to pass to foreign server.
	 */
	i = 0;
	foreach(lc, grouping_target->exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);
		Index		sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell   *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any of the GROUP BY expression is not shippable we can not
			 * push down aggregation to the foreign server.
			 */
			if (!mysql_is_foreign_expr(root, grouped_rel, expr, true))
				return false;

			/*
			 * If it would be a foreign param, we can't put it into the tlist,
			 * so we have to fail.
			 */
			if (mysql_is_foreign_param(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/* Check entire expression whether it is pushable or not */
			if (mysql_is_foreign_expr(root, grouped_rel, expr, true) &&
				!mysql_is_foreign_param(root, grouped_rel, expr))
			{
				/* Pushable, add to tlist */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				List	   *aggvars;

				/* Not matched exactly, pull the var with aggregates then */
				aggvars = pull_var_clause((Node *) expr,
										  PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the foreign server.  (We
				 * don't have to check is_foreign_param, since that certainly
				 * won't return true for any such expression.)
				 */
				if (!mysql_is_foreign_expr(root, grouped_rel, (Expr *) aggvars, true))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain var
				 * nodes should be either same as some GROUP BY expression or
				 * part of some GROUP BY expression. In later case, the query
				 * cannot refer plain var nodes without the surrounding
				 * expression.  In both the cases, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * adding pulled plain var nodes in SELECT clause will cause
				 * an error on the foreign server if they are not same as some
				 * GROUP BY expression.
				 */
				foreach(l, aggvars)
				{
					Expr	   *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable having clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
#if PG_VERSION_NUM >= 110000
	if (havingQual)
	{
		ListCell   *lc;

		foreach(lc, (List *) havingQual)
#elif PG_VERSION_NUM >= 100000
	if (root->hasHavingQual && query->havingQual)
	{
		ListCell	*lc;
		foreach(lc, (List *) query->havingQual)
#endif
		{
			Expr	   *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
#if PG_VERSION_NUM >= 140000
			rinfo = make_restrictinfo(root,
									  expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#else
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
#endif

			if (!mysql_is_foreign_expr(root, grouped_rel, expr, true))
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
			else
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List	   *aggvars = NIL;
		ListCell   *lc;

		foreach(lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause,
												  PVC_INCLUDE_AGGREGATES));
		}

		foreach(lc, aggvars)
		{
			Expr	   *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.  Again, we need not check
			 * is_foreign_param for a foreign aggregate.
			 */
			if (IsA(expr, Aggref))
			{
				if (!mysql_is_foreign_expr(root, grouped_rel, expr, true))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)",
					 ofpinfo->relation_name->data);

	return true;
}

/*
 * mysqlGetForeignUpperPaths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
#if PG_VERSION_NUM >= 110000
static void
mysqlGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						  RelOptInfo *input_rel, RelOptInfo *output_rel,
						  void *extra)
#elif PG_VERSION_NUM >= 100000
static void
mysqlGetForeignUpperPaths(PlannerInfo *root, UpperRelationKind stage,
						  RelOptInfo *input_rel, RelOptInfo *output_rel)
#endif
{
	MySQLFdwRelationInfo *fpinfo;

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the foreign server.
	 */
	if (!input_rel->fdw_private ||
		!((MySQLFdwRelationInfo *) input_rel->fdw_private)->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls. */
	if (stage != UPPERREL_GROUP_AGG || output_rel->fdw_private)
		return;

	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	fpinfo->pushdown_safe = false;
	output_rel->fdw_private = fpinfo;

#if PG_VERSION_NUM >= 110000
	mysql_add_foreign_grouping_paths(root, input_rel, output_rel,
									 (GroupPathExtraData *) extra);
#elif PG_VERSION_NUM >= 100000
	mysql_add_foreign_grouping_paths(root, input_rel, output_rel);
#endif
}

/*
 * mysql_add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
#if PG_VERSION_NUM >= 110000
static void
mysql_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								 RelOptInfo *grouped_rel,
								 GroupPathExtraData *extra)
#elif PG_VERSION_NUM >= 100000
static void
mysql_add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel,
								 RelOptInfo *grouped_rel)
#endif
{
	Query	   *parse = root->parse;
	MySQLFdwRelationInfo *fpinfo = grouped_rel->fdw_private;
	ForeignPath *grouppath;
	Cost		startup_cost;
	Cost		total_cost;
	double		num_groups;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs &&
		!root->hasHavingQual)
		return;

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/* Assess if it is safe to push down aggregation and grouping. */
#if PG_VERSION_NUM >= 110000
	if (!mysql_foreign_grouping_ok(root, grouped_rel, extra->havingQual))
#elif PG_VERSION_NUM >= 100000
	if (!mysql_foreign_grouping_ok(root, grouped_rel))
#endif
		return;

	/*
	 * TODO: Put accurate estimates here.
	 *
	 * Cost used here is minimum of the cost estimated for base and join
	 * relation.
	 */
	startup_cost = 15;
	total_cost = 10 + startup_cost;

	/* Estimate output tuples which should be same as number of groups */
#if PG_VERSION_NUM >= 140000
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rel->rows, NULL, NULL);
#else
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rel->rows, NULL);
#endif

	/* Create and add foreign path to the grouping relation. */
#if PG_VERSION_NUM >= 120000
	grouppath = create_foreign_upper_path(root,
										  grouped_rel,
										  grouped_rel->reltarget,
										  num_groups,
										  startup_cost,
										  total_cost,
										  NIL,	/* no pathkeys */
										  NULL,
										  NIL); /* no fdw_private */
#elif PG_VERSION_NUM >= 110000
	grouppath = create_foreignscan_path(root,
										grouped_rel,
										grouped_rel->reltarget,
										num_groups,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										grouped_rel->lateral_relids,
										NULL,
										NIL);	/* no fdw_private */
#else
	grouppath = create_foreignscan_path(root,
										grouped_rel,
										root->upper_targets[UPPERREL_GROUP_AGG],
										num_groups,
										startup_cost,
										total_cost,
										NIL,	/* no pathkeys */
										grouped_rel->lateral_relids,
										NULL,
										NIL);	/* no fdw_private */
#endif

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, (Path *) grouppath);
}
