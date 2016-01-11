/*-------------------------------------------------------------------------
 *
 * mysql_fdw.c
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "mysql_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>

#include <mysql.h>
#include <errmsg.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/planmain.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"

#include "mysql_query.h"

#define DEFAULTE_NUM_ROWS    1000

PG_MODULE_MAGIC;


typedef struct MySQLFdwRelationInfo
{
	/* baserestrictinfo clauses, broken down into safe and unsafe subsets. */
	List	   *remote_conds;
	List	   *local_conds;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

} MySQLFdwRelationInfo;


extern Datum mysql_fdw_handler(PG_FUNCTION_ARGS);
extern PGDLLEXPORT void _PG_init(void);

bool mysql_load_library(void);
static void mysql_fdw_exit(int code, Datum arg);

PG_FUNCTION_INFO_V1(mysql_fdw_handler);

/*
 * FDW callback routines
 */
static void mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void mysqlBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *mysqlIterateForeignScan(ForeignScanState *node);
static void mysqlReScanForeignScan(ForeignScanState *node);
static void mysqlEndForeignScan(ForeignScanState *node);

static List *mysqlPlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation,
									int subplan_index);
static void mysqlBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
									List *fdw_private, int subplan_index, int eflags);
static TupleTableSlot *mysqlExecForeignInsert(EState *estate, ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot, TupleTableSlot *planSlot);
static void mysqlAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte,
										 Relation target_relation);
static TupleTableSlot * mysqlExecForeignUpdate(EState *estate, ResultRelInfo *resultRelInfo,
											   TupleTableSlot *slot,TupleTableSlot *planSlot);
static TupleTableSlot *mysqlExecForeignDelete(EState *estate, ResultRelInfo *resultRelInfo,
											  TupleTableSlot *slot, TupleTableSlot *planSlot);
static void mysqlEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo);

static void mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static bool mysqlAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
static ForeignScan *mysqlGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
										ForeignPath *best_path, List * tlist, List *scan_clauses
#if PG_VERSION_NUM >= 90500
		                                ,Plan * outer_plan
#endif
);
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost,
							   Oid foreigntableid);

#if PG_VERSION_NUM >= 90500
static List *mysqlImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
#endif

static bool mysql_is_column_unique(Oid foreigntableid);

void* mysql_dll_handle = NULL;
static int wait_timeout = WAIT_TIMEOUT;
static int interactive_timeout = INTERACTIVE_TIMEOUT;

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
#if defined(__APPLE__)
	/*
	 * Mac OS/BSD does not support RTLD_DEEPBIND, but it still
	 * works without the RTLD_DEEPBIND
	 */
	mysql_dll_handle = dlopen(_MYSQL_LIBNAME, RTLD_LAZY);
#else
	mysql_dll_handle = dlopen(_MYSQL_LIBNAME, RTLD_LAZY | RTLD_DEEPBIND);
#endif
	if(mysql_dll_handle == NULL)
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
	_mysql_real_connect = dlsym(mysql_dll_handle, "mysql_real_connect");
	_mysql_close = dlsym(mysql_dll_handle, "mysql_close");
	_mysql_init = dlsym(mysql_dll_handle, "mysql_init");
	_mysql_stmt_attr_set = dlsym(mysql_dll_handle, "mysql_stmt_attr_set");
	_mysql_store_result = dlsym(mysql_dll_handle, "mysql_store_result");
	_mysql_stmt_errno = dlsym(mysql_dll_handle, "mysql_stmt_errno");
	_mysql_errno = dlsym(mysql_dll_handle, "mysql_errno");
	_mysql_num_fields = dlsym(mysql_dll_handle, "mysql_num_fields");
	_mysql_num_rows = dlsym(mysql_dll_handle, "mysql_num_rows");

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
		_mysql_real_connect == NULL ||
		_mysql_close == NULL ||
		_mysql_init == NULL ||
		_mysql_stmt_attr_set == NULL ||
		_mysql_store_result == NULL ||
		_mysql_stmt_errno == NULL ||
		_mysql_errno == NULL ||
		_mysql_num_fields == NULL ||
		_mysql_num_rows == NULL)
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
				errhint("export LD_LIBRARY_PATH to locate the library")));

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
 * mysql_fdw_exit: Exit callback function.
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

	/* Callback functions for readable FDW */
	fdwroutine->GetForeignRelSize = mysqlGetForeignRelSize;
	fdwroutine->GetForeignPaths = mysqlGetForeignPaths;
	fdwroutine->AnalyzeForeignTable = mysqlAnalyzeForeignTable;
	fdwroutine->GetForeignPlan = mysqlGetForeignPlan;
	fdwroutine->ExplainForeignScan = mysqlExplainForeignScan;
	fdwroutine->BeginForeignScan = mysqlBeginForeignScan;
	fdwroutine->IterateForeignScan = mysqlIterateForeignScan;
	fdwroutine->ReScanForeignScan = mysqlReScanForeignScan;
	fdwroutine->EndForeignScan = mysqlEndForeignScan;

#if PG_VERSION_NUM >= 90500
	fdwroutine->ImportForeignSchema = mysqlImportForeignSchema;
#endif

	/* Callback functions for writeable FDW */
	fdwroutine->ExecForeignInsert = mysqlExecForeignInsert;
	fdwroutine->BeginForeignModify = mysqlBeginForeignModify;
	fdwroutine->PlanForeignModify = mysqlPlanForeignModify;
	fdwroutine->AddForeignUpdateTargets = mysqlAddForeignUpdateTargets;
	fdwroutine->ExecForeignUpdate = mysqlExecForeignUpdate;
	fdwroutine->ExecForeignDelete = mysqlExecForeignDelete;
	fdwroutine->EndForeignModify = mysqlEndForeignModify;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * mysqlBeginForeignScan: Initiate access to the database
 */
static void
mysqlBeginForeignScan(ForeignScanState *node, int eflags)
{
	TupleTableSlot    *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc         tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	MYSQL             *conn = NULL;
	RangeTblEntry     *rte;
	MySQLFdwExecState *festate = NULL;
	EState            *estate = node->ss.ps.state;
	ForeignScan       *fsplan = (ForeignScan *) node->ss.ps.plan;
	mysql_opt         *options;
	ListCell          *lc = NULL;
	int               atindex = 0;
	unsigned long     prefetch_rows = MYSQL_PREFETCH_ROWS;
	unsigned long     type = (unsigned long) CURSOR_TYPE_READ_ONLY;
	Oid               userid;
	ForeignServer     *server;
	UserMapping       *user;
	ForeignTable      *table;
	char              timeout[255];

	/*
	 * We'll save private state in node->fdw_state.
	 */
	festate = (MySQLFdwExecState *) palloc(sizeof(MySQLFdwExecState));
	node->fdw_state = (void *) festate;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	rte = rt_fetch(fsplan->scan.scanrelid, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	festate->rel = node->ss.ss_currentRelation;
	table = GetForeignTable(RelationGetRelid(festate->rel));
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch the options */
	options = mysql_get_options(RelationGetRelid(node->ss.ss_currentRelation));

	/*
	 * Get the already connected connection, otherwise connect
	 * and get the connection handle.
	 */
	conn = mysql_get_connection(server, user, options);

	/* Stash away the state info we have already */
	festate->query = strVal(list_nth(fsplan->fdw_private, 0));
	festate->retrieved_attrs = list_nth(fsplan->fdw_private, 1);
	festate->conn = conn;

	festate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);

	if (wait_timeout > 0)
	{
		/* Set the session timeout in seconds*/
		sprintf(timeout, "SET wait_timeout = %d", wait_timeout);
		_mysql_query(festate->conn, timeout);
	}

	if (interactive_timeout > 0)
	{
		/* Set the session timeout in seconds*/
		sprintf(timeout, "SET interactive_timeout = %d", interactive_timeout);
		_mysql_query(festate->conn, timeout);
	}

	_mysql_query(festate->conn, "SET time_zone = '+00:00'");
	_mysql_query(festate->conn, "SET sql_mode='ANSI_QUOTES'");


	/* Initialize the MySQL statement */
	festate->stmt = _mysql_stmt_init(festate->conn);
	if (festate->stmt == NULL)
	{
		char *err = pstrdup(_mysql_error(festate->conn));
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to initialize the mysql query: \n%s", err)));
	}

	/* Prepare MySQL statement */
	if (_mysql_stmt_prepare(festate->stmt, festate->query, strlen(festate->query)) != 0)
	{
		switch(_mysql_stmt_errno(festate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(festate->conn));
				mysql_rel_connection(festate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to prepare the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(festate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to prepare the MySQL query: \n%s", err)));
			}
			break;
		}
	}

    /* int column_count = mysql_num_fields(festate->meta); */

	/* Set the statement as cursor type */
	_mysql_stmt_attr_set(festate->stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);

	/* Set the pre-fetch rows */
	_mysql_stmt_attr_set(festate->stmt, STMT_ATTR_PREFETCH_ROWS, (void*) &prefetch_rows);

	festate->table = (mysql_table*) palloc0(sizeof(mysql_table));
	festate->table->column = (mysql_column *) palloc0(sizeof(mysql_column) * tupleDescriptor->natts);
	festate->table->_mysql_bind = (MYSQL_BIND*) palloc0(sizeof(MYSQL_BIND) * tupleDescriptor->natts);

	festate->table->_mysql_res = _mysql_stmt_result_metadata(festate->stmt);
	if (NULL == festate->table->_mysql_res)
	{
			char *err = pstrdup(_mysql_error(festate->conn));
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					 errmsg("failed to retrieve query result set metadata: \n%s", err)));
	}

    festate->table->_mysql_fields = _mysql_fetch_fields(festate->table->_mysql_res);

	foreach(lc, festate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc) - 1;
		Oid pgtype = tupleDescriptor->attrs[attnum]->atttypid;
		int32 pgtypmod = tupleDescriptor->attrs[attnum]->atttypmod;

		if (tupleDescriptor->attrs[attnum]->attisdropped)
			continue;

		festate->table->column[atindex]._mysql_bind = &festate->table->_mysql_bind[atindex];

		mysql_bind_result(pgtype, pgtypmod, &festate->table->_mysql_fields[atindex],
							&festate->table->column[atindex]);
		atindex++;
	}

	/* Bind the results pointers for the prepare statements */
	if (_mysql_stmt_bind_result(festate->stmt, festate->table->_mysql_bind) != 0)
	{
		switch(_mysql_stmt_errno(festate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(festate->conn));
				mysql_rel_connection(festate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to bind the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(festate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to bind the MySQL query: \n%s", err)));
			}
			break;
		}
	}
	/*
	 * Finally execute the query and result will be placed in the
	 * array we already bind
	 */
	if (_mysql_stmt_execute(festate->stmt) != 0)
	{
		switch(_mysql_stmt_errno(festate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(festate->conn));
				mysql_rel_connection(festate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg(" 7 failed to execute the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(festate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
		}
	}
}

/*
 * mysqlIterateForeignScan: Iterate and get the rows one by one from
 * MySQL and placed in tuple slot
 */
static TupleTableSlot *
mysqlIterateForeignScan(ForeignScanState *node)
{
	MySQLFdwExecState   *festate = (MySQLFdwExecState *) node->fdw_state;
	TupleTableSlot      *tupleSlot = node->ss.ss_ScanTupleSlot;
	TupleDesc           tupleDescriptor = tupleSlot->tts_tupleDescriptor;
	int                 attid = 0;
	ListCell            *lc = NULL;
	int                 rc = 0;

	memset (tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset (tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);

	ExecClearTuple(tupleSlot);

	attid = 0;
	rc = _mysql_stmt_fetch(festate->stmt);
	if (0 == rc)
	{
		foreach(lc, festate->retrieved_attrs)
		{
			int attnum = lfirst_int(lc) - 1;
			Oid pgtype = tupleDescriptor->attrs[attnum]->atttypid;
			int32 pgtypmod = tupleDescriptor->attrs[attnum]->atttypmod;

			tupleSlot->tts_isnull[attnum] = festate->table->column[attid].is_null;
			if (!festate->table->column[attid].is_null)
				tupleSlot->tts_values[attnum] = mysql_convert_to_pg(pgtype, pgtypmod,
                                                                    &festate->table->column[attid]);

			attid++;
		}
		ExecStoreVirtualTuple(tupleSlot);
	}
	else if (1 == rc)
	{
			/*
			  Error occurred. Error code and message can be obtained
			  by calling mysql_stmt_errno() and mysql_stmt_error().
			*/
	}
	else if (MYSQL_NO_DATA == rc)
	{
            /*
              No more rows/data exists
            */
	}
	else if (MYSQL_DATA_TRUNCATED == rc)
	{
            /* Data truncation occurred */
            /*
              MYSQL_DATA_TRUNCATED is returned when truncation
              reporting is enabled. To determine which column values
              were truncated when this value is returned, check the
              error members of the MYSQL_BIND structures used for
              fetching values. Truncation reporting is enabled by
              default, but can be controlled by calling
              mysql_options() with the MYSQL_REPORT_DATA_TRUNCATION
              option.
            */
	}
	return tupleSlot;
}


/*
 * mysqlExplainForeignScan: Produce extra output for EXPLAIN
 */
static void
mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;
	mysql_opt *options;

	/* Fetch options */
	options = mysql_get_options(RelationGetRelid(node->ss.ss_currentRelation));

	/* Give some possibly useful info about startup costs */
	if (es->verbose)
	{
		if (strcmp(options->svr_address, "127.0.0.1") == 0 || strcmp(options->svr_address, "localhost") == 0)
			ExplainPropertyLong("Local server startup cost", 10, es);
		else
			ExplainPropertyLong("Remote server startup cost", 25, es);

		ExplainPropertyText("Remote query", festate->query, es);
	}
}

/*
 * mysqlEndForeignScan: Finish scanning foreign table and dispose
 * objects used for this scan
 */
static void
mysqlEndForeignScan(ForeignScanState *node)
{
	MySQLFdwExecState *festate = (MySQLFdwExecState *) node->fdw_state;

    if (festate->table)
    {
         if (festate->table->_mysql_res) {
               _mysql_free_result(festate->table->_mysql_res);
               festate->table->_mysql_res = NULL;
         }
       }

	if (festate->stmt)
	{
		_mysql_stmt_close(festate->stmt);
		festate->stmt = NULL;
	}
}

/*
 * mysqlReScanForeignScan: Rescan table, possibly with new parameters
 */
static void
mysqlReScanForeignScan(ForeignScanState *node)
{
	/* TODO: Need to implement rescan */
}

/*
 * mysqlGetForeignRelSize: Create a FdwPlan for a scan on the foreign table
 */
static void
mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	StringInfoData       sql;
	double               rows = 0;
	double               filtered = 0;
	MYSQL                *conn = NULL;
	MYSQL_RES            *result = NULL;
	MYSQL_ROW            row;
	Bitmapset            *attrs_used = NULL;
	List                 *retrieved_attrs = NULL;
	mysql_opt            *options = NULL;
	Oid                  userid =  GetUserId();
	ForeignServer        *server;
	UserMapping          *user;
	ForeignTable         *table;
	MySQLFdwRelationInfo *fpinfo;
	ListCell             *lc;
	MYSQL_FIELD          *field;
	int                  i;
	int                  num_fields;
	List                *params_list = NULL;

	fpinfo = (MySQLFdwRelationInfo *) palloc0(sizeof(MySQLFdwRelationInfo));
	baserel->fdw_private = (void *) fpinfo;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch options */
	options = mysql_get_options(foreigntableid);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	_mysql_query(conn, "SET sql_mode='ANSI_QUOTES'");

	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &attrs_used);

	foreach(lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

		if (is_foreign_expr(root, baserel, ri->clause))
			fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
		else
			fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
	}

	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &fpinfo->attrs_used);
	foreach(lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
	}

	if (options->use_remote_estimate)
	{
		initStringInfo(&sql);
		appendStringInfo(&sql, "EXPLAIN ");

		mysql_deparse_select(&sql, root, baserel, fpinfo->attrs_used, options->svr_table, &retrieved_attrs);

		if (fpinfo->remote_conds)
			mysql_append_where_clause(&sql, root, baserel, fpinfo->remote_conds,
						  true, &params_list);

		if (_mysql_query(conn, sql.data) != 0)
		{
			switch(_mysql_errno(conn))
			{
				case CR_NO_ERROR:
					break;

				case CR_OUT_OF_MEMORY:
				case CR_SERVER_GONE_ERROR:
				case CR_SERVER_LOST:
				case CR_UNKNOWN_ERROR:
				{
					char *err = pstrdup(_mysql_error(conn));
					mysql_rel_connection(conn);
					ereport(ERROR,
								(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
								errmsg("failed to execute the MySQL query: \n%s", err)));
				}
				break;
				case CR_COMMANDS_OUT_OF_SYNC:
				default:
				{
					char *err = pstrdup(_mysql_error(conn));
					ereport(ERROR,
								(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
								errmsg("failed to execute the MySQL query: \n%s", err)));
				}
			}
		}
		result = _mysql_store_result(conn);
		if (result)
		{
			/*
			 * MySQL provide numbers of rows per table invole in
			 * the statment, but we don't have problem with it
			 * because we are sending separate query per table
			 * in FDW.
			 */
			row = _mysql_fetch_row(result);
			num_fields = _mysql_num_fields(result);
			if (row)
			{
				for (i = 0; i < num_fields; i++)
				{
					field = _mysql_fetch_field(result);
					if (strcmp(field->name, "rows") == 0)
					{
						if (row[i])
							rows = atof(row[i]);
					}
					else if (strcmp(field->name, "filtered") == 0)
					{
						if (row[i])
							filtered = atof(row[i]);
					}
				}
			}
			_mysql_free_result(result);
		}
	}
	if (rows > 0)
		rows = ((rows + 1) * filtered) / 100;
	else
		rows  = DEFAULTE_NUM_ROWS;

	baserel->rows = rows;
	baserel->tuples = rows;
}


static bool
mysql_is_column_unique(Oid foreigntableid)
{
	StringInfoData       sql;
	MYSQL                *conn = NULL;
	MYSQL_RES            *result = NULL;
	MYSQL_ROW            row;
	mysql_opt            *options = NULL;
	Oid                  userid =  GetUserId();
	ForeignServer        *server;
	UserMapping          *user;
	ForeignTable         *table;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch the options */
	options = mysql_get_options(foreigntableid);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	/* Build the query */
	initStringInfo(&sql);

	appendStringInfo(&sql, "EXPLAIN %s", options->svr_table);
	if (_mysql_query(conn, sql.data) != 0)
	{
		switch(_mysql_errno(conn))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			case CR_UNKNOWN_ERROR:
			{
				char *err = pstrdup(_mysql_error(conn));
				mysql_rel_connection(conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			default:
			{
				char *err = pstrdup(_mysql_error(conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
		}
	}

	result = _mysql_store_result(conn);
	if (result)
	{
		int num_fields = _mysql_num_fields(result);
		row = _mysql_fetch_row(result);
		if (row && num_fields > 3)
		{
			if ((strcmp(row[3], "PRI") == 0) || (strcmp(row[3], "UNI")) == 0)
			{
				_mysql_free_result(result);
				return true;
			}
		}
		_mysql_free_result(result);
	}
	return false;
}

/*
 * mysqlEstimateCosts: Estimate the remote query cost
 */
static void
mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid)
{
	mysql_opt *options;

	/* Fetch options */
	options = mysql_get_options(foreigntableid);

	/* Local databases are probably faster */
	if (strcmp(options->svr_address, "127.0.0.1") == 0 || strcmp(options->svr_address, "localhost") == 0)
		*startup_cost = 10;
	else
		*startup_cost = 25;

	*total_cost = baserel->rows + *startup_cost;
}


/*
 * mysqlGetForeignPaths: Get the foreign paths
 */
static void
mysqlGetForeignPaths(PlannerInfo *root,RelOptInfo *baserel,Oid foreigntableid)
{
	Cost startup_cost;
	Cost total_cost;

	/* Estimate costs */
	mysqlEstimateCosts(root, baserel, &startup_cost, &total_cost, foreigntableid);

	/* Create a ForeignPath node and add it as only possible path */
	add_path(baserel, (Path *)
			 create_foreignscan_path(root, baserel,
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 NULL,	/* no outer rel either */
#if PG_VERSION_NUM >= 90500
									 NULL,	/* no extra plan */
#endif
									 NULL));	/* no fdw_private data */
}


/*
 * mysqlGetForeignPlan: Get a foreign scan plan node
 */
static ForeignScan *
mysqlGetForeignPlan(
		PlannerInfo *root
		,RelOptInfo *baserel
		,Oid foreigntableid
		,ForeignPath *best_path
		,List * tlist
		,List *scan_clauses
#if PG_VERSION_NUM >= 90500
		,Plan * outer_plan
#endif
)
{
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) baserel->fdw_private;
	Index          scan_relid = baserel->relid;
	List           *fdw_private;
	List           *local_exprs = NULL;
	List           *params_list = NULL;
	List           *remote_conds = NIL;

	StringInfoData sql;
	mysql_opt      *options;
	List           *retrieved_attrs;
	ListCell       *lc;

	/* Fetch options */
	options = mysql_get_options(foreigntableid);

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */

	/* Build the query */
	initStringInfo(&sql);

	/*
	 * Separate the scan_clauses into those that can be executed remotely and
	 * those that can't.  baserestrictinfo clauses that were previously
	 * determined to be safe or unsafe by classifyConditions are shown in
	 * fpinfo->remote_conds and fpinfo->local_conds.  Anything else in the
	 * scan_clauses list will be a join clause, which we have to check for
	 * remote-safety.
	 *
	 * Note: the join clauses we see here should be the exact same ones
	 * previously examined by postgresGetForeignPaths.  Possibly it'd be worth
	 * passing forward the classification work done then, rather than
	 * repeating it here.
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
		else if (is_foreign_expr(root, baserel, rinfo->clause))
			remote_conds = lappend(remote_conds, rinfo);
		else
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	mysql_deparse_select(&sql, root, baserel, fpinfo->attrs_used, options->svr_table, &retrieved_attrs);

	if (remote_conds)
		mysql_append_where_clause(&sql, root, baserel, remote_conds,
						  true, &params_list);

	if (baserel->relid == root->parse->resultRelation &&
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

	/*
	 * Create the ForeignScan node from target list, local filtering
	 * expressions, remote parameter expressions, and FDW private information.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist
	                       ,local_exprs
	                       ,scan_relid
	                       ,params_list
	                       ,fdw_private
#if PG_VERSION_NUM >= 90500
	                       ,NIL
	                       ,NIL
	                       ,outer_plan
#endif
	                       );
}

/*
 * mysqlAnalyzeForeignTable: Implement stats collection
 */
static bool
mysqlAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	StringInfoData sql;
	double         table_size = 0;
	MYSQL          *conn;
	MYSQL_RES      *result;
	MYSQL_ROW      row;
	Oid            foreignTableId = RelationGetRelid(relation);
	mysql_opt      *options;
	char           *relname;
	ForeignServer  *server;
	UserMapping    *user;
	ForeignTable   *table;

	table = GetForeignTable(foreignTableId);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(relation->rd_rel->relowner, server->serverid);

	/* Fetch options */
	options = mysql_get_options(foreignTableId);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	/* Build the query */
	initStringInfo(&sql);

	/* If no table name specified, use the foreign table name */
	relname = options->svr_table;
	if ( relname == NULL)
			relname = RelationGetRelationName(relation);

	mysql_deparse_analyze(&sql, options->svr_database, relname);

	if (_mysql_query(conn, sql.data) != 0)
	{
		switch(_mysql_errno(conn))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(conn));
				mysql_rel_connection(conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
		}
	}
	result = _mysql_store_result(conn);
	if (result)
	{
		row = _mysql_fetch_row(result);
		table_size = atof(row[0]);
		_mysql_free_result(result);
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

	CmdType         operation = plan->operation;
	RangeTblEntry   *rte = planner_rt_fetch(resultRelation, root);
	Relation        rel;
	List            *targetAttrs = NULL;
	StringInfoData  sql;
	char            *attname;
	Oid             foreignTableId;

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	foreignTableId = RelationGetRelid(rel);

	if (!mysql_is_column_unique(foreignTableId))
		elog(ERROR, "first column of remote table must be unique for INSERT/UPDATE/DELETE operation");

	if (operation == CMD_INSERT)
	{
		TupleDesc tupdesc = RelationGetDescr(rel);
		int attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
#if PG_VERSION_NUM >= 90500
		Bitmapset *tmpset = bms_copy(rte->updatedCols);
#else
		Bitmapset *tmpset = bms_copy(rte->modifiedCols);
#endif
		AttrNumber	col;

		while ((col = bms_first_member(tmpset)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
			if (col <= InvalidAttrNumber)		/* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			/*
			 * We also disallow updates to the first column
			 */
			if (col == 1)	/* shouldn't happen */
				elog(ERROR, "row identifier column update is not supported");

			targetAttrs = lappend_int(targetAttrs, col);
		}
		/* We also want the rowid column to be available for the update */
		targetAttrs = lcons_int(1, targetAttrs);
	}
	else
	{
		targetAttrs = lcons_int(1, targetAttrs);
	}

	attname = get_relid_attribute_name(foreignTableId, 1);

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			mysql_deparse_insert(&sql, root, resultRelation, rel, targetAttrs);
			break;
		case CMD_UPDATE:
			mysql_deparse_update(&sql, root, resultRelation, rel, targetAttrs, attname);
			break;
		case CMD_DELETE:
			mysql_deparse_delete(&sql, root, resultRelation, rel, attname);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	if (plan->returningLists)
		elog(ERROR, "RETURNING is not supported by this FDW");

	heap_close(rel, NoLock);
	return list_make2(makeString(sql.data), targetAttrs);
}


/*
 * mysqlBeginForeignModify: Begin an insert/update/delete operation
 * on a foreign table
 */
static void
mysqlBeginForeignModify(ModifyTableState *mtstate,
						ResultRelInfo *resultRelInfo,
						List *fdw_private,
						int subplan_index,
						int eflags)
{
	MySQLFdwExecState   *fmstate = NULL;
	EState              *estate = mtstate->ps.state;
	Relation            rel = resultRelInfo->ri_RelationDesc;
	AttrNumber          n_params = 0;
	Oid                 typefnoid = InvalidOid;
	bool                isvarlena = false;
	ListCell            *lc = NULL;
	Oid                 foreignTableId = InvalidOid;
	RangeTblEntry       *rte;
	Oid                 userid;
	ForeignServer       *server;
	UserMapping         *user;
	ForeignTable        *table;

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

	/* Begin constructing MongoFdwModifyState. */
	fmstate = (MySQLFdwExecState *) palloc0(sizeof(MySQLFdwExecState));

	fmstate->rel = rel;
	fmstate->mysqlFdwOptions = mysql_get_options(foreignTableId);
	fmstate->conn = mysql_get_connection(server, user, fmstate->mysqlFdwOptions);

	fmstate->query = strVal(list_nth(fdw_private, 0));
	fmstate->retrieved_attrs = (List *) list_nth(fdw_private, 1);

	n_params = list_length(fmstate->retrieved_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "mysql_fdw temporary data",
											  ALLOCSET_SMALL_MINSIZE,
											  ALLOCSET_SMALL_INITSIZE,
											  ALLOCSET_SMALL_MAXSIZE);

	/* Set up for remaining transmittable parameters */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc);
		Form_pg_attribute attr = RelationGetDescr(rel)->attrs[attnum - 1];

		Assert(!attr->attisdropped);

		getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}
	Assert(fmstate->p_nums <= n_params);

	n_params = list_length(fmstate->retrieved_attrs);

	/* Initialize mysql statment */
	fmstate->stmt = _mysql_stmt_init(fmstate->conn);
	if (!fmstate->stmt)
	{
		char *err = pstrdup(_mysql_error(fmstate->conn));
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to initialize the MySQL query: \n%s", err)
				 ));
	}

	/* Prepare mysql statment */
	if (_mysql_stmt_prepare(fmstate->stmt, fmstate->query, strlen(fmstate->query)) != 0)
	{
		switch(_mysql_stmt_errno(fmstate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				mysql_rel_connection(fmstate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to prepare the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to prepare the MySQL query: \n%s", err)));
			}
			break;
		}
	}
	resultRelInfo->ri_FdwState = fmstate;
}


/*
 * mysqlExecForeignInsert: Insert one row into a foreign table
 */
static TupleTableSlot *
mysqlExecForeignInsert(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState   *fmstate;
	MYSQL_BIND          *mysql_bind_buffer = NULL;
	ListCell            *lc;
	Datum               value = 0;
	int                 n_params = 0;
	MemoryContext       oldcontext;

	fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	n_params = list_length(fmstate->retrieved_attrs);

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	mysql_bind_buffer = (MYSQL_BIND*) palloc0(sizeof(MYSQL_BIND) * n_params);

	_mysql_query(fmstate->conn, "SET time_zone = '+00:00'");
	_mysql_query(fmstate->conn, "SET sql_mode='ANSI_QUOTES'");

	foreach(lc, fmstate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc) - 1;

		bool *isnull = (bool*) palloc0(sizeof(bool) * n_params);
		Oid type = slot->tts_tupleDescriptor->attrs[attnum]->atttypid;

		value = slot_getattr(slot, attnum + 1, &isnull[attnum]);

		mysql_bind_sql_var(type, attnum, value, mysql_bind_buffer, &isnull[attnum]);
	}

	/* Bind values */
	if (_mysql_stmt_bind_param(fmstate->stmt, mysql_bind_buffer) != 0)
	{
		switch(_mysql_stmt_errno(fmstate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				mysql_rel_connection(fmstate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to bind the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to bind the MySQL query: \n%s", err)));
			}
			break;
		}
	}
	/* Execute the query */
	if (_mysql_stmt_execute(fmstate->stmt) != 0)
	{
		switch(_mysql_stmt_errno(fmstate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				mysql_rel_connection(fmstate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
		}
	}
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
	Relation          rel = resultRelInfo->ri_RelationDesc;
	MYSQL_BIND        *mysql_bind_buffer = NULL;
	Oid               foreignTableId = RelationGetRelid(rel);
	bool              is_null = false;
	ListCell          *lc = NULL;
	int               bindnum = 0;
	Oid               typeoid;
	Datum             value = 0;
	int               n_params = 0;
	bool              *isnull = NULL;
	int               i = 0;

	n_params = list_length(fmstate->retrieved_attrs);

	mysql_bind_buffer = (MYSQL_BIND*) palloc0(sizeof(MYSQL_BIND) * n_params);
	isnull = palloc0(sizeof(bool) * n_params);

	/* Bind the values */
	foreach(lc, fmstate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc);
		Oid type;

		/* first attribute cannot be in target list attribute */
		if (attnum == 1)
			continue;

		type = slot->tts_tupleDescriptor->attrs[attnum - 1]->atttypid;
		value = slot_getattr(slot, attnum, (bool*)(&isnull[i]));

		mysql_bind_sql_var(type, bindnum, value, mysql_bind_buffer, &isnull[i]);
		bindnum++;
		i++;
	}

	/* Get the id that was passed up as a resjunk column */
	value = ExecGetJunkAttribute(planSlot, 1, &is_null);
	typeoid = get_atttype(foreignTableId, 1);

	/* Bind qual */
	mysql_bind_sql_var(typeoid, bindnum, value, mysql_bind_buffer, &is_null);

	if (_mysql_stmt_bind_param(fmstate->stmt, mysql_bind_buffer) != 0)
	{
		char *err = pstrdup(_mysql_error(fmstate->conn));
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to bind the MySQL query: %s", err)
				 ));
	}

	if (_mysql_stmt_execute(fmstate->stmt) != 0)
	{
		switch(_mysql_stmt_errno(fmstate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				mysql_rel_connection(fmstate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
		}
	}
	/* Return NULL if nothing was updated on the remote end */
	return slot;
}


/*
 * mysqlAddForeignUpdateTargets: Add column(s) needed for update/delete on a foreign table,
 * we are using first column as row identification column, so we are adding that into target
 * list.
 */
static void
mysqlAddForeignUpdateTargets(Query *parsetree,
							 RangeTblEntry *target_rte,
							 Relation target_relation)
{
	Var         *var = NULL;
	const char  *attrname = NULL;
	TargetEntry *tle = NULL;

	/*
	 * What we need is the rowid which is the first column
	 */
	Form_pg_attribute attr =
	RelationGetDescr(target_relation)->attrs[0];

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
				  1,
				  attr->atttypid,
				  attr->atttypmod,
				  InvalidOid,
				  0);

	/* Wrap it in a TLE with the right name ... */
	attrname = NameStr(attr->attname);

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}


/*
 * MongoExecForeignDelete: Delete one row from a foreign table
 */
static TupleTableSlot *
mysqlExecForeignDelete(EState *estate,
					   ResultRelInfo *resultRelInfo,
					   TupleTableSlot *slot,
					   TupleTableSlot *planSlot)
{
	MySQLFdwExecState    *fmstate = (MySQLFdwExecState *) resultRelInfo->ri_FdwState;
	Relation             rel = resultRelInfo->ri_RelationDesc;
	MYSQL_BIND           *mysql_bind_buffer = NULL;
	Oid                  foreignTableId = RelationGetRelid(rel);
	bool                 is_null = false;
	int                  bindnum = 0;
	Oid                  typeoid;
	Datum                value = 0;

	mysql_bind_buffer = (MYSQL_BIND*) palloc0(sizeof(MYSQL_BIND) * 1);

	/* Get the id that was passed up as a resjunk column */
	value = ExecGetJunkAttribute(planSlot, 1, &is_null);
	typeoid = get_atttype(foreignTableId, 1);

	/* Bind qual */
	mysql_bind_sql_var(typeoid, bindnum, value, mysql_bind_buffer, &is_null);

	if (_mysql_stmt_bind_param(fmstate->stmt, mysql_bind_buffer) != 0)
	{
		char *err = pstrdup(_mysql_error(fmstate->conn));
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("failed to execute the MySQL query: %s", err)
				 ));
	}

	if (_mysql_stmt_execute(fmstate->stmt) != 0)
	{
		switch(_mysql_stmt_errno(fmstate->stmt))
		{
			case CR_NO_ERROR:
				break;

			case CR_OUT_OF_MEMORY:
			case CR_SERVER_GONE_ERROR:
			case CR_SERVER_LOST:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				mysql_rel_connection(fmstate->conn);
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
			case CR_COMMANDS_OUT_OF_SYNC:
			case CR_UNKNOWN_ERROR:
			default:
			{
				char *err = pstrdup(_mysql_error(fmstate->conn));
				ereport(ERROR,
							(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
							errmsg("failed to execute the MySQL query: \n%s", err)));
			}
			break;
		}
	}
	/* Return NULL if nothing was updated on the remote end */
	return slot;
}

/*
 * MongoEndForeignModify
 *		Finish an insert/update/delete operation on a foreign table
 */
static void
mysqlEndForeignModify(EState *estate, ResultRelInfo *resultRelInfo)
{
	MySQLFdwExecState *festate = resultRelInfo->ri_FdwState;

	if (festate && festate->stmt)
	{
		_mysql_stmt_close(festate->stmt);
		festate->stmt = NULL;
	}
}

/*
 * Import a foreign schema (9.5+)
 */
#if PG_VERSION_NUM >= 90500
static List *
mysqlImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List           *commands = NIL;
    bool           import_default = false;
    bool           import_not_null = true;
    ForeignServer  *server;
    UserMapping    *user;
    mysql_opt      *options = NULL;
    MYSQL          *conn;
    StringInfoData buf;
    MYSQL_RES      *volatile res = NULL;
    MYSQL_ROW      row;
    ListCell       *lc;
    char           *err = NULL;

    /* Parse statement options */
    foreach(lc, stmt->options)
    {
        DefElem *def = (DefElem *) lfirst(lc);

        if (strcmp(def->defname, "import_default") == 0)
            import_default = defGetBoolean(def);
        else if (strcmp(def->defname, "import_not_null") == 0)
            import_not_null = defGetBoolean(def);
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
    options = mysql_get_options(serverOid);
    conn = mysql_get_connection(server, user, options);

    /* Create workspace for strings */
    initStringInfo(&buf);

    /* Check that the schema really exists */
    appendStringInfo(&buf, "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s'", stmt->remote_schema);

    if (_mysql_query(conn, buf.data) != 0)
    {
        switch(_mysql_errno(conn))
        {
            case CR_NO_ERROR:
                break;

            case CR_OUT_OF_MEMORY:
            case CR_SERVER_GONE_ERROR:
            case CR_SERVER_LOST:
            case CR_UNKNOWN_ERROR:
                err = pstrdup(_mysql_error(conn));
                mysql_rel_connection(conn);
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("failed to execute the MySQL query: \n%s", err)));
                break;

            case CR_COMMANDS_OUT_OF_SYNC:
            default:
                err = pstrdup(_mysql_error(conn));
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("failed to execute the MySQL query: \n%s", err)));
        }
    }

    res = _mysql_store_result(conn);
    if (!res || _mysql_num_rows(res) < 1)
    {
        ereport(ERROR,
                (errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
                 errmsg("schema \"%s\" is not present on foreign server \"%s\"",
                        stmt->remote_schema, server->servername)));
    }
    _mysql_free_result(res);
    res = NULL;
    resetStringInfo(&buf);

    /*
     * Fetch all table data from this schema, possibly restricted by
     * EXCEPT or LIMIT TO.
     */
    appendStringInfo(&buf,
                     " SELECT"
                     "  t.TABLE_NAME,"
                     "  c.COLUMN_NAME,"
                     "  CASE"
                     "    WHEN c.DATA_TYPE = 'enum' THEN LOWER(CONCAT(c.COLUMN_NAME, '_t'))"
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
                     "    WHEN c.DATA_TYPE = 'blob' THEN 'bytea'"
                     "    WHEN c.DATA_TYPE = 'mediumblob' THEN 'bytea'"
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
        bool first_item = true;

        appendStringInfoString(&buf, " AND t.TABLE_NAME ");
        if (stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
            appendStringInfoString(&buf, "NOT ");
        appendStringInfoString(&buf, "IN (");

        /* Append list of table names within IN clause */
        foreach(lc, stmt->table_list)
        {
            RangeVar *rv = (RangeVar *) lfirst(lc);

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
    if (_mysql_query(conn, buf.data) != 0)
    {
        switch(_mysql_errno(conn))
        {
            case CR_NO_ERROR:
                break;

            case CR_OUT_OF_MEMORY:
            case CR_SERVER_GONE_ERROR:
            case CR_SERVER_LOST:
            case CR_UNKNOWN_ERROR:
                err = pstrdup(_mysql_error(conn));
                mysql_rel_connection(conn);
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("failed to execute the MySQL query: \n%s", err)));
                break;

            case CR_COMMANDS_OUT_OF_SYNC:
            default:
                err = pstrdup(_mysql_error(conn));
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                         errmsg("failed to execute the MySQL query: \n%s", err)));
        }
    }

    res = _mysql_store_result(conn);
    row = _mysql_fetch_row(res);
    while (row)
    {
        char *tablename = row[0];
        bool first_item = true;

        resetStringInfo(&buf);
        appendStringInfo(&buf, "CREATE FOREIGN TABLE %s (\n",
                         quote_identifier(tablename));

        /* Scan all rows for this table */
        do
        {
            char *attname;
            char *typename;
            char *typedfn;
            char *attnotnull;
            char *attdefault;

            /* If table has no columns, we'll see nulls here */
            if (row[1] == NULL)
                continue;

            attname = row[1];
            typename = row[2];
            typedfn = row[3];
            attnotnull = row[4];
            attdefault = row[5] == NULL ? (char *) NULL : row[5];

            if (strncmp(typedfn, "enum(", 5) == 0)
                ereport(NOTICE, (errmsg("If you encounter an error, you may need to execute the following first:\n"
                                        "DO $$BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_type WHERE typname = '%s') THEN CREATE TYPE %s AS %s; END IF; END$$;\n",
                                        typename,
                                        typename,
                                        typedfn)));

            if (first_item)
                first_item = false;
            else
                appendStringInfoString(&buf, ",\n");

            /* Print column name and type */
            appendStringInfo(&buf, "  %s %s",
                             quote_identifier(attname),
                             typename);

            /* Add DEFAULT if needed */
            if (import_default && attdefault != NULL)
                appendStringInfo(&buf, " DEFAULT %s", attdefault);

            /* Add NOT NULL if needed */
            if (import_not_null && attnotnull[0] == 't')
                appendStringInfoString(&buf, " NOT NULL");
        }
        while ((row = _mysql_fetch_row(res)) &&
               (strcmp(row[0], tablename) == 0));

        /*
         * Add server name and table-level options.  We specify remote
         * database and table name as options (the latter to ensure that
         * renaming the foreign table doesn't break the association).
         */
        appendStringInfo(&buf, "\n) SERVER %s OPTIONS (dbname '%s', table_name '%s');\n",
                         quote_identifier(server->servername),
                         stmt->remote_schema,
                         tablename);

        commands = lappend(commands, pstrdup(buf.data));
    }

    /* Clean up */
    _mysql_free_result(res);
    res = NULL;
    resetStringInfo(&buf);

    mysql_rel_connection(conn);

    return commands;
}
#endif
