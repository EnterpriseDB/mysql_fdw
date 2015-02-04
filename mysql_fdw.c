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

PG_MODULE_MAGIC;

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
										ForeignPath *best_path, List * tlist, List *scan_clauses);
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost,
							   Oid foreigntableid);

#ifdef __APPLE__
	#define _MYSQL_LIBNAME "libmysqlclient.dylib"
#else
	#define _MYSQL_LIBNAME "libmysqlclient.so"
#endif

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
#ifdef __APPLE__
	/*
	 * Mac OS does not support RTLD_DEEPBIND, but it still
	 * works without the RTLD_DEEPBIND on Mac OS
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
		_mysql_errno == NULL)
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

	/* Callback functions for readable FDW */
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
	MYSQL_BIND        *mysql_result_buffer = NULL;
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

	/* Allocate memory for the values and nulls for the results */
	festate->tts_values = (Datum*) palloc0(sizeof(Datum) * tupleDescriptor->natts);
	festate->tts_isnull = palloc0(sizeof(bool) * tupleDescriptor->natts);

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

	/* Set the statement as cursor type */
	_mysql_stmt_attr_set(festate->stmt, STMT_ATTR_CURSOR_TYPE, (void*) &type);

	/* Set the pre-fetch rows */
	_mysql_stmt_attr_set(festate->stmt, STMT_ATTR_PREFETCH_ROWS, (void*) &prefetch_rows);

	mysql_result_buffer = (MYSQL_BIND*) palloc0(sizeof(MYSQL_BIND) * tupleDescriptor->natts);
	foreach(lc, festate->retrieved_attrs)
	{
		int attnum = lfirst_int(lc) - 1;

		if (tupleDescriptor->attrs[attnum]->attisdropped)
			continue;

		festate->tts_values[atindex] = mysql_bind_result(atindex, &festate->tts_values[atindex],
												   (bool*)&festate->tts_isnull[atindex], mysql_result_buffer);
		atindex++;
	}

	/* Bind the results pointers for the prepare statements */
	if (_mysql_stmt_bind_result(festate->stmt, mysql_result_buffer) != 0)
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

	memset (tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
	memset (tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);

	ExecClearTuple(tupleSlot);

	attid = 0;
	if (_mysql_stmt_fetch(festate->stmt) == 0)
	{
		foreach(lc, festate->retrieved_attrs)
		{
			int attnum = lfirst_int(lc) - 1;
			Oid pgtype = tupleDescriptor->attrs[attnum]->atttypid;
			int32 pgtypmod = tupleDescriptor->attrs[attnum]->atttypmod;

			tupleSlot->tts_isnull[attnum] = festate->tts_isnull[attid];
			if (!festate->tts_isnull[attid])
				tupleSlot->tts_values[attnum] = mysql_convert_to_pg(pgtype, pgtypmod,
																	festate->tts_values[attid], festate);

			attid++;
		}
		ExecStoreVirtualTuple(tupleSlot);
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
	if (festate->stmt)
	{
		_mysql_stmt_close(festate->stmt);
		festate->stmt = NULL;
	}

	if (festate->query)
	{
		pfree(festate->query);
		festate->query = 0;
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
	StringInfoData  sql;
	double          rows = 0;
	MYSQL           *conn = NULL;
	MYSQL_RES       *result = NULL;
	MYSQL_ROW       row;
	Bitmapset       *attrs_used = NULL;
	List            *retrieved_attrs = NULL;
	mysql_opt       *options = NULL;
	Oid             userid =  GetUserId();
	ForeignServer   *server;
	UserMapping     *user;
	ForeignTable    *table;

	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	user = GetUserMapping(userid, server->serverid);

	/* Fetch the options */
	options = mysql_get_options(foreigntableid);


	/* Fetch options */
	options = mysql_get_options(foreigntableid);

	/* Connect to the server */
	conn = mysql_get_connection(server, user, options);

	_mysql_query(conn, "SET sql_mode='ANSI_QUOTES'");

	/* Build the query */
	initStringInfo(&sql);

	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &attrs_used);

	appendStringInfo(&sql, "EXPLAIN ");
	mysql_deparse_select(&sql, root, baserel, attrs_used, options->svr_table, &retrieved_attrs);

	/*
	 * TODO: MySQL seems to have some pretty unhelpful EXPLAIN output, which only
	 * gives a row estimate for each relation in the statement. We'll use the
	 * sum of the rows as our cost estimate - it's not great (in fact, in some
	 * cases it sucks), but it's all we've got for now.
	 */
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
		while ((row = _mysql_fetch_row(result)))
			rows += row[8] ? atof(row[8]) : 2;

		_mysql_free_result(result);
	}
	baserel->rows = rows;
	baserel->tuples = rows;
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
									 NULL,	/* no pathkeys */
									 NULL,	/* no outer rel either */
									 NULL));	/* no fdw_private data */
}


/*
 * mysqlGetForeignPlan: Get a foreign scan plan node
 */
static ForeignScan *
mysqlGetForeignPlan(PlannerInfo *root,RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List * tlist, List *scan_clauses)
{
	Index          scan_relid = baserel->relid;
	List           *fdw_private;
	List           *local_exprs = NULL;
	List           *params_list = NULL;
	StringInfoData sql;
	mysql_opt      *options;
	Bitmapset      *attrs_used = NULL;
	List           *retrieved_attrs;

	/* Fetch options */
	options = mysql_get_options(foreigntableid);

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */

	/* Build the query */
	initStringInfo(&sql);

	pull_varattnos((Node *) baserel->reltargetlist, baserel->relid, &attrs_used);

	mysql_deparse_select(&sql, root, baserel, attrs_used, options->svr_table, &retrieved_attrs);

	if (scan_clauses)
		mysql_append_where_clause(&sql, root, baserel, scan_clauses,
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
	return make_foreignscan(tlist,
							local_exprs,
							scan_relid,
							params_list,
							fdw_private);
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
		Bitmapset *tmpset = bms_copy(rte->modifiedCols);
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

	foreignTableId = RelationGetRelid(rel);
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

