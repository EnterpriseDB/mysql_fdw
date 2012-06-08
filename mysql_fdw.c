/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for MySQL
 *
 * Copyright (c) 2011, PostgreSQL Global Development Group
 *
 * This software is released under the PostgreSQL Licence
 *
 * Author: Dave Page <dpage@pgadmin.org>
 *
 * IDENTIFICATION
 *		  mysql_fdw/mysql_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#define list_length mysql_list_length
#define list_delete mysql_list_delete
#define list_free mysql_list_free
#include <mysql.h>
#undef list_length
#undef list_delete
#undef list_free

#include "funcapi.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "mb/pg_wchar.h"
#include "optimizer/cost.h"
#include "storage/fd.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#if (PG_VERSION_NUM >= 90200)
#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"
#endif

PG_MODULE_MAGIC;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MySQLFdwOption
{
	const char	*optname;
	Oid		optcontext;	/* Oid of catalog in which option may appear */
};

/*
 * Valid options for mysql_fdw.
 *
 */
static struct MySQLFdwOption valid_options[] =
{

	/* Connection options */
	{ "address",		ForeignServerRelationId },
	{ "port",		ForeignServerRelationId },
	{ "username",		UserMappingRelationId },
	{ "password",		UserMappingRelationId },
	{ "database",		ForeignTableRelationId },
	{ "query",		ForeignTableRelationId },
	{ "table",		ForeignTableRelationId },

	/* Sentinel */
	{ NULL,			InvalidOid }
};

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */

typedef struct MySQLFdwExecutionState
{
	MYSQL		*conn;
	MYSQL_RES	*result;
	char		*query;
} MySQLFdwExecutionState;

/*
 * SQL functions
 */
extern Datum mysql_fdw_handler(PG_FUNCTION_ARGS);
extern Datum mysql_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mysql_fdw_handler);
PG_FUNCTION_INFO_V1(mysql_fdw_validator);

/*
 * FDW callback routines
 */
static void mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void mysqlBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *mysqlIterateForeignScan(ForeignScanState *node);
static void mysqlReScanForeignScan(ForeignScanState *node);
static void mysqlEndForeignScan(ForeignScanState *node);
#if (PG_VERSION_NUM >= 90200)
static void mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static bool mysqlAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
static ForeignScan *mysqlGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List * tlist, List *scan_clauses);
#else
static FdwPlan *mysqlPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif

/*
 * Helper functions
 */
static bool mysqlIsValidOption(const char *option, Oid context);
static void mysqlGetOptions(Oid foreigntableid, char **address, int *port, char **username, char **password, char **database, char **query, char **table);
#if (PG_VERSION_NUM >= 90200)
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid);
#endif

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
mysql_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	
	#if (PG_VERSION_NUM >= 90200)
	fdwroutine->GetForeignRelSize = mysqlGetForeignRelSize;
	fdwroutine->GetForeignPaths = mysqlGetForeignPaths;
	fdwroutine->AnalyzeForeignTable = mysqlAnalyzeForeignTable;
	fdwroutine->GetForeignPlan = mysqlGetForeignPlan;
	#else
	fdwroutine->PlanForeignScan = mysqlPlanForeignScan;
	#endif
	
	fdwroutine->ExplainForeignScan = mysqlExplainForeignScan;
	fdwroutine->BeginForeignScan = mysqlBeginForeignScan;
	fdwroutine->IterateForeignScan = mysqlIterateForeignScan;
	fdwroutine->ReScanForeignScan = mysqlReScanForeignScan;
	fdwroutine->EndForeignScan = mysqlEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses file_fdw.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
Datum
mysql_fdw_validator(PG_FUNCTION_ARGS)
{
	List		*options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid		catalog = PG_GETARG_OID(1);
	char		*svr_address = NULL;
	int		svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char		*svr_database = NULL;
	char		*svr_query = NULL;
	char		*svr_table = NULL;
	ListCell	*cell;

	/*
	 * Check that only options supported by mysql_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	   *def = (DefElem *) lfirst(cell);

		if (!mysqlIsValidOption(def->defname, catalog))
		{
			struct MySQLFdwOption *opt;
			StringInfoData buf;

			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with list of valid options for the object.
			 */
			initStringInfo(&buf);
			for (opt = valid_options; opt->optname; opt++)
			{
				if (catalog == opt->optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",
							 opt->optname);
			}

			ereport(ERROR, 
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME), 
				errmsg("invalid option \"%s\"", def->defname), 
				errhint("Valid options in this context are: %s", buf.len ? buf.data : "<none>")
				));
		}

		if (strcmp(def->defname, "address") == 0)
		{
			if (svr_address)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR), 
					errmsg("conflicting or redundant options: address (%s)", defGetString(def))
					));

			svr_address = defGetString(def);
		}
		else if (strcmp(def->defname, "port") == 0)
		{
			if (svr_port)
				ereport(ERROR, 
					(errcode(ERRCODE_SYNTAX_ERROR), 
					errmsg("conflicting or redundant options: port (%s)", defGetString(def))
					));

			svr_port = atoi(defGetString(def));
		}
		if (strcmp(def->defname, "username") == 0)
		{
			if (svr_username)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting or redundant options: username (%s)", defGetString(def))
					));

			svr_username = defGetString(def);
		}
		if (strcmp(def->defname, "password") == 0)
		{
			if (svr_password)
				ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting or redundant options: password")
					));

			svr_password = defGetString(def);
		}
		else if (strcmp(def->defname, "database") == 0)
		{
			if (svr_database)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting or redundant options: database (%s)", defGetString(def))
					));

			svr_database = defGetString(def);
		}
		else if (strcmp(def->defname, "query") == 0)
		{
			if (svr_table)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting options: query cannot be used with table")
					));

			if (svr_query)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting or redundant options: query (%s)", defGetString(def))
					));

			svr_query = defGetString(def);
		}
		else if (strcmp(def->defname, "table") == 0)
		{
			if (svr_query)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting options: table cannot be used with query")
					));

			if (svr_table)
				ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("conflicting or redundant options: table (%s)", defGetString(def))
					));

			svr_table = defGetString(def);
		}
	}

	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
mysqlIsValidOption(const char *option, Oid context)
{
	struct MySQLFdwOption *opt;

	for (opt = valid_options; opt->optname; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->optname, option) == 0)
			return true;
	}
	return false;
}

/*
 * Fetch the options for a mysql_fdw foreign table.
 */
static void
mysqlGetOptions(Oid foreigntableid, char **address, int *port, char **username, char **password, char **database, char **query, char **table)
{
	ForeignTable	*f_table;
	ForeignServer	*f_server;
	UserMapping	*f_mapping;
	List		*options;
	ListCell	*lc;

	/*
	 * Extract options from FDW objects.
	 */
	f_table = GetForeignTable(foreigntableid);
	f_server = GetForeignServer(f_table->serverid);
	f_mapping = GetUserMapping(GetUserId(), f_table->serverid);

	options = NIL;
	options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "address") == 0)
			*address = defGetString(def);

		if (strcmp(def->defname, "port") == 0)
			*port = atoi(defGetString(def));

		if (strcmp(def->defname, "username") == 0)
			*username = defGetString(def);

		if (strcmp(def->defname, "password") == 0)
			*password = defGetString(def);

		if (strcmp(def->defname, "database") == 0)
			*database = defGetString(def);

		if (strcmp(def->defname, "query") == 0)
			*query = defGetString(def);

		if (strcmp(def->defname, "table") == 0)
			*table = defGetString(def);
	}

	/* Default values, if required */
	if (!*address)
		*address = "127.0.0.1";

	if (!*port)
		*port = 3306;

	/* Check we have the options we need to proceed */
	if (!*table && !*query)
		ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("either a table or a query must be specified")
			));
}

#if (PG_VERSION_NUM < 90200)
/*
 * (9.1) Create a FdwPlan for a scan on the foreign table
 */
static FdwPlan *
mysqlPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel)
{
	FdwPlan		*fdwplan;
	char		*svr_address = NULL;
	int		svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char 		*svr_database = NULL;
	char 		*svr_query = NULL;
	char 		*svr_table = NULL;
	char		*query;
	double		rows;
	MYSQL		*conn;
	MYSQL_RES	*result;
	MYSQL_ROW	row;

	/* Fetch options  */
	mysqlGetOptions(foreigntableid, &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_query, &svr_table);

	/* Construct FdwPlan with cost estimates. */
	fdwplan = makeNode(FdwPlan);

	/* Local databases are probably faster */
	if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0)
		fdwplan->startup_cost = 10;
	else
		fdwplan->startup_cost = 25;

	/* 
	 * TODO: Find a way to stash this connection object away, so we don't have
	 * to reconnect to MySQL again later.
	 */

	/* Connect to the server */
	conn = mysql_init(NULL);
	if (!conn)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
			errmsg("failed to initialise the MySQL connection object")
			));

	if (!mysql_real_connect(conn, svr_address, svr_username, svr_password, svr_database, svr_port, NULL, 0))
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			errmsg("failed to connect to MySQL: %s", mysql_error(conn))
			));

	/* Build the query */
	if (svr_query)
	{
		size_t len = strlen(svr_query) + 9;

		query = (char *) palloc(len);
		snprintf(query, len, "EXPLAIN %s", svr_query);
	}
	else
	{
		size_t len = strlen(svr_table) + 23;

		query = (char *) palloc(len);
		snprintf(query, len, "EXPLAIN SELECT * FROM %s", svr_table);
	}

	/*A
	 * MySQL seems to have some pretty unhelpful EXPLAIN output, which only
	 * gives a row estimate for each relation in the statement. We'll use the
	 * sum of the rows as our cost estimate - it's not great (in fact, in some
	 * cases it sucks), but it's all we've got for now.
	 */
	if (mysql_query(conn, query) != 0)
	{
		char *err = pstrdup(mysql_error(conn));
		mysql_close(conn);
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			errmsg("failed to execute the MySQL query: %s", err)
			));
	}

	result = mysql_store_result(conn);

	while ((row = mysql_fetch_row(result)))
		rows += atof(row[8]);

	mysql_free_result(result);
	mysql_close(conn);

	baserel->rows = rows;
	baserel->tuples = rows;
	fdwplan->total_cost = rows + fdwplan->startup_cost;
	fdwplan->fdw_private = NIL;	/* not used */

	return fdwplan;
}
#endif

/*
 * Produce extra output for EXPLAIN
 */
static void
mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	char		    *svr_address = NULL;
	int		    svr_port = 0;
	char		    *svr_username = NULL;
	char		    *svr_password = NULL;
	char		    *svr_database = NULL;
	char		    *svr_query = NULL;
	char		    *svr_table = NULL;

	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;

	/* Fetch options  */
	mysqlGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_query, &svr_table);

	/* Give some possibly useful info about startup costs */
	if (es->costs)
	{
		if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0)	
			ExplainPropertyLong("Local server startup cost", 10, es);
		else
			ExplainPropertyLong("Remote server startup cost", 25, es);
		ExplainPropertyText("MySQL query", festate->query, es);
	}
}

/*
 * Initiate access to the database
 */
static void
mysqlBeginForeignScan(ForeignScanState *node, int eflags)
{
	char			*svr_address = NULL;
	int			svr_port = 0;
	char			*svr_username = NULL;
	char			*svr_password = NULL;
	char			*svr_database = NULL;
	char			*svr_query = NULL;
	char			*svr_table = NULL;
	MYSQL			*conn;
	MySQLFdwExecutionState  *festate;
	char			*query;

	/* Fetch options  */
	mysqlGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_query, &svr_table);

	/* Connect to the server */
	conn = mysql_init(NULL);
	if (!conn)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
			errmsg("failed to initialise the MySQL connection object")
			));

	if (!mysql_real_connect(conn, svr_address, svr_username, svr_password, svr_database, svr_port, NULL, 0))
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			errmsg("failed to connect to MySQL: %s", mysql_error(conn))
			));

	/* Build the query */
	if (svr_query)
		query = svr_query;
	else
	{
		size_t len = strlen(svr_table) + 15;

		query = (char *)palloc(len);
		snprintf(query, len, "SELECT * FROM %s", svr_table);
	}

	/* Stash away the state info we have already */
	festate = (MySQLFdwExecutionState *) palloc(sizeof(MySQLFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->conn = conn;
	festate->result = NULL;
	festate->query = query;
}

/*
 * Read next record from the data file and store it into the
 * ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
mysqlIterateForeignScan(ForeignScanState *node)
{
	char			**values;
	HeapTuple		tuple;
	MYSQL_ROW		row;
	int			x;

	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/* Execute the query, if required */
	if (!festate->result)
	{
		if (mysql_query(festate->conn, festate->query) != 0)
		{
			char *err = pstrdup(mysql_error(festate->conn));
			mysql_close(festate->conn);
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to execute the MySQL query: %s", err)
				));
		}

		/* Guess the query succeeded then */
		festate->result = mysql_store_result(festate->conn);
	}

	/* Cleanup */
	ExecClearTuple(slot);

	/* Get the next tuple */
	if ((row = mysql_fetch_row(festate->result)))
	{
		/* Build the tuple */
		values = (char **) palloc(sizeof(char *) * mysql_num_fields(festate->result));

		for (x = 0; x < mysql_num_fields(festate->result); x++)
			values[x] = row[x];

		tuple = BuildTupleFromCStrings(TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att), values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	return slot;
}

/*
 * Finish scanning foreign table and dispose objects used for this scan
 */
static void
mysqlEndForeignScan(ForeignScanState *node)
{
	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;

	if (festate->result)
	{
		mysql_free_result(festate->result);
		festate->result = NULL;
	}

	if (festate->conn)
	{
		mysql_close(festate->conn);
		festate->conn = NULL;
	}

	if (festate->query)
	{
		pfree(festate->query);
		festate->query = 0;
	}
}

/*
 * Rescan table, possibly with new parameters
 */
static void
mysqlReScanForeignScan(ForeignScanState *node)
{
	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;

	if (festate->result)
	{
		mysql_data_seek(festate->result, 0);
	}
}

#if (PG_VERSION_NUM >= 90200)
/*
 * (9.2+) Create a FdwPlan for a scan on the foreign table
 */
static void 
mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	char		*svr_address = NULL;
	int		svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char 		*svr_database = NULL;
	char 		*svr_query = NULL;
	char 		*svr_table = NULL;
	char		*query;
	double		rows = 0;
	MYSQL		*conn;
	MYSQL_RES	*result;
	MYSQL_ROW	row;

	/* Fetch options  */
	mysqlGetOptions(foreigntableid, &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_query, &svr_table);

	/* Construct FdwPlan with cost estimates. */

	/* 
	 * TODO: Find a way to stash this connection object away, so we don't have
	 * to reconnect to MySQL aain later.
	 */

	/* Connect to the server */
	conn = mysql_init(NULL);
	if (!conn)
		ereport(ERROR,
			(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
			errmsg("failed to initialise the MySQL connection object")
			));

	if (!mysql_real_connect(conn, svr_address, svr_username, svr_password, svr_database, svr_port, NULL, 0))
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
			errmsg("failed to connect to MySQL: %s", mysql_error(conn))
			));

	/* Build the query */
	if (svr_query)
	{
		size_t len = strlen(svr_query) + 9;

		query = (char *) palloc(len);
		snprintf(query, len, "EXPLAIN %s", svr_query);
	}
	else
	{
		size_t len = strlen(svr_table) + 23;

		query = (char *) palloc(len);
		snprintf(query, len, "EXPLAIN SELECT * FROM %s", svr_table);
	}

	/*A
	 * MySQL seems to have some pretty unhelpful EXPLAIN output, which only
	 * gives a row estimate for each relation in the statement. We'll use the
	 * sum of the rows as our cost estimate - it's not great (in fact, in some
	 * cases it sucks), but it's all we've got for now.
	 */
	if (mysql_query(conn, query) != 0)
	{
		char *err = pstrdup(mysql_error(conn));
		mysql_close(conn);
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			errmsg("failed to execute the MySQL query: %s", err)
			));
	}

	result = mysql_store_result(conn);

	while ((row = mysql_fetch_row(result)))
		rows += atof(row[8]);

	mysql_free_result(result);
	mysql_close(conn);

	baserel->rows = rows;
	baserel->tuples = rows;
}

/*
 * (9.2+) Estimate the remote query cost
 */
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid)
{
        char		*svr_address = NULL;
	int		svr_port = 0;
	char		*svr_username = NULL;
	char		*svr_password = NULL;
	char 		*svr_database = NULL;
	char 		*svr_query = NULL;
	char 		*svr_table = NULL;

	/* Fetch options  */
	mysqlGetOptions(foreigntableid, &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_query, &svr_table);

       /* Local databases are probably faster */
       if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0)
           *startup_cost = 10;
       else
           *startup_cost = 25;

       *total_cost = baserel->rows + *startup_cost;
} 

/*
 * (9.2+) Get the foreign paths
 */
static void mysqlGetForeignPaths(PlannerInfo *root,RelOptInfo *baserel,Oid foreigntableid)
{
       Cost        startup_cost;
       Cost        total_cost;

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
                                     NIL));	/* no fdw_private data */
}

/*
 * (9.2+) Get a foreign scan plan node
 */
static ForeignScan * mysqlGetForeignPlan(PlannerInfo *root,RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List * tlist, List *scan_clauses) 	
{
        Index scan_relid = baserel->relid;

        scan_clauses = extract_actual_clauses(scan_clauses, false);

        /* Create the ForeignScan node */
        return make_foreignscan(tlist,
                            scan_clauses,
                            scan_relid,
                            NIL,	/* no expressions to evaluate */
                            NIL);	/* no private state either */
}

/* 
 * FIXME: (9.2+) Implement stats collection 
 */
static bool mysqlAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
        return false;
}	
#endif

