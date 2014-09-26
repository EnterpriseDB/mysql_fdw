/*-------------------------------------------------------------------------
 *
 *		  foreign-data wrapper for MySQL
 *
 * Copyright (c) 2011 - 2013, PostgreSQL Global Development Group
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
#include "mysql_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/reloptions.h"
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


PG_MODULE_MAGIC;

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
static void mysql_fdw_exit(int code, Datum arg);
extern PGDLLEXPORT void _PG_init(void);

PG_FUNCTION_INFO_V1(mysql_fdw_handler);

/*
 * FDW callback routines
 */
static void mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void mysqlBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *mysqlIterateForeignScan(ForeignScanState *node);
static void mysqlReScanForeignScan(ForeignScanState *node);
static void mysqlEndForeignScan(ForeignScanState *node);
static void mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void mysqlGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static bool mysqlAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
static ForeignScan *mysqlGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List * tlist, List *scan_clauses);

/*
 * Helper functions
 */
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid);


/*
 * Library load-time initalization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void
_PG_init(void)
{
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
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
mysql_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);
	
	fdwroutine->GetForeignRelSize = mysqlGetForeignRelSize;
	fdwroutine->GetForeignPaths = mysqlGetForeignPaths;
	fdwroutine->AnalyzeForeignTable = mysqlAnalyzeForeignTable;
	fdwroutine->GetForeignPlan = mysqlGetForeignPlan;
	
	fdwroutine->ExplainForeignScan = mysqlExplainForeignScan;
	fdwroutine->BeginForeignScan = mysqlBeginForeignScan;
	fdwroutine->IterateForeignScan = mysqlIterateForeignScan;
	fdwroutine->ReScanForeignScan = mysqlReScanForeignScan;
	fdwroutine->EndForeignScan = mysqlEndForeignScan;

	PG_RETURN_POINTER(fdwroutine);
}


/*
 * Produce extra output for EXPLAIN
 */
static void
mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;
	mysql_opt *options;

	/* Fetch options  */
	options = mysql_get_options(RelationGetRelid(node->ss.ss_currentRelation));

	/* Give some possibly useful info about startup costs */
	if (es->costs)
	{
		if (strcmp(options->svr_address, "127.0.0.1") == 0 || strcmp(options->svr_address, "localhost") == 0)
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
	MYSQL			*conn;
	MySQLFdwExecutionState  *festate;
	char			*query;
	mysql_opt *options;
	size_t len;

	/* Fetch options  */
	options = mysql_get_options(RelationGetRelid(node->ss.ss_currentRelation));

	/* Connect to the server */
	conn = mysql_get_connection(options);

	/* Build the query */
	len = strlen(options->svr_table) + 15;

	query = (char *)palloc(len);
	snprintf(query, len, "SELECT * FROM %s", options->svr_table);

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
		mysql_query(festate->conn, "SET time_zone = '+00:00'");
		if (mysql_query(festate->conn, festate->query) != 0)
		{
			char *err = pstrdup(mysql_error(festate->conn));
			ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("failed to execute the MySQL query: %s", err)
				));
		}

		/* Guess the query succeeded then */
		festate->result = mysql_use_result(festate->conn);
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

/*
 * (9.2+) Create a FdwPlan for a scan on the foreign table
 */
static void 
mysqlGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	char		*query;
	double		rows = 0;
	MYSQL		*conn;
	MYSQL_RES	*result;
	MYSQL_ROW	row;
	mysql_opt *options;
	size_t len;

	/* Fetch options  */
	options = mysql_get_options(foreigntableid);

	/* Construct FdwPlan with cost estimates. */

		/* Connect to the server */
	conn = mysql_get_connection(options);

	len = strlen(options->svr_table) + 23;

	query = (char *) palloc(len);
	snprintf(query, len, "EXPLAIN SELECT * FROM %s", options->svr_table);

	/*A
	 * MySQL seems to have some pretty unhelpful EXPLAIN output, which only
	 * gives a row estimate for each relation in the statement. We'll use the
	 * sum of the rows as our cost estimate - it's not great (in fact, in some
	 * cases it sucks), but it's all we've got for now.
	 */
	if (mysql_query(conn, query) != 0)
	{
		char *err = pstrdup(mysql_error(conn));
		ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			errmsg("failed to execute the MySQL query: %s", err)
			));
	}

	result = mysql_store_result(conn);

	while ((row = mysql_fetch_row(result)))
		rows += row[8] ? atof(row[8]) : 2;

	mysql_free_result(result);

	baserel->rows = rows;
	baserel->tuples = rows;
}

/*
 * (9.2+) Estimate the remote query cost
 */
static void mysqlEstimateCosts(PlannerInfo *root, RelOptInfo *baserel, Cost *startup_cost, Cost *total_cost, Oid foreigntableid)
{
	mysql_opt *options;

	/* Fetch options  */
	options = mysql_get_options(foreigntableid);

       /* Local databases are probably faster */
       if (strcmp(options->svr_address, "127.0.0.1") == 0 || strcmp(options->svr_address, "localhost") == 0)
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

