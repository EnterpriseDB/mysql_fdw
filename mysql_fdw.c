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

/* Debug mode */
/* #define DEBUG */

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

PG_MODULE_MAGIC;

#define PROCID_TEXTEQ 67

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
	{ "schema",		ForeignTableRelationId },
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
	AttInMetadata	*attinmeta;
	long long	row;
	char		*address;
	int		port;
	char		*username;
	char		*password;
	char		*database;
	char		*schema;
	char		*table;
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
static FdwPlan *mysqlPlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
static void mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void mysqlBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *mysqlIterateForeignScan(ForeignScanState *node);
static void mysqlReScanForeignScan(ForeignScanState *node);
static void mysqlEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static bool mysqlIsValidOption(const char *option, Oid context);
static void mysqlGetOptions(Oid foreigntableid, char **address, int *port, char **username, char **password, char **database, char **schema, char **table);
static void mysqlGetQual(Node *node, TupleDesc tupdesc, char **key, char **value, bool *pushdown);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to my callback routines.
 */
Datum
mysql_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#ifdef DEBUG
	elog(NOTICE, "mysql_fdw_handler");
#endif

	fdwroutine->PlanForeignScan = mysqlPlanForeignScan;
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
	char		*svr_schema = NULL;
	char		*svr_table = NULL;
	ListCell	*cell;

#ifdef DEBUG
	elog(NOTICE, "mysql_fdw_validator");
#endif

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
                else if (strcmp(def->defname, "schema") == 0)
                {
                        if (svr_schema)
                                ereport(ERROR,
                                        (errcode(ERRCODE_SYNTAX_ERROR),
                                        errmsg("conflicting or redundant options: schema (%s)", defGetString(def))
                                        ));

                        svr_schema = defGetString(def);
                }
                else if (strcmp(def->defname, "table") == 0)
                {
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

#ifdef DEBUG
	elog(NOTICE, "mysqlIsValidOption");
#endif

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
mysqlGetOptions(Oid foreigntableid, char **address, int *port, char **username, char **password, char **database, char **schema, char **table)
{
	ForeignTable	*f_table;
	ForeignServer	*f_server;
	UserMapping	*f_mapping;
	List		*options;
	ListCell	*lc;

#ifdef DEBUG
	elog(NOTICE, "mysqlGetOptions");
#endif

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

                if (strcmp(def->defname, "schema") == 0)
                        *schema = defGetString(def);

                if (strcmp(def->defname, "table") == 0)
                        *table = defGetString(def);
	}

	/* Default values, if required */
	if (!*address)
		*address = "127.0.0.1";

	if (!*port)
		*port = 3306;
}

/*
 * mysqlPlanForeignScan
 *		Create a FdwPlan for a scan on the foreign table
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
	char 		*svr_schema = NULL;
	char 		*svr_table = NULL;

#ifdef DEBUG
	elog(NOTICE, "mysqlPlanForeignScan");
#endif

	/* Fetch options  */
	mysqlGetOptions(foreigntableid, &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_schema, &svr_table);

	/* Connect to the database */

	/* Execute a query to get the database size */

	/* Construct FdwPlan with cost estimates. */
	fdwplan = makeNode(FdwPlan);

	/* Local databases are probably faster */
	if (strcmp(svr_address, "127.0.0.1") == 0 || strcmp(svr_address, "localhost") == 0)
		fdwplan->startup_cost = 10;
	else
		fdwplan->startup_cost = 25;

	baserel->rows = 10;
	fdwplan->total_cost = 10 + fdwplan->startup_cost;
	fdwplan->fdw_private = NIL;	/* not used */

	return fdwplan;
}

/*
 * fileExplainForeignScan
 *		Produce extra output for EXPLAIN
 */
static void
mysqlExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "mysqlExplainForeignScan");
#endif

	/* Execute a query to get the database size */

	/* Suppress file size if we're not showing cost details */
	if (es->costs)
	{
		ExplainPropertyLong("Foreign MySQL Database Size", 10, es);
	}
}

/*
 * mysqlBeginForeignScan
 *		Initiate access to the database
 */
static void
mysqlBeginForeignScan(ForeignScanState *node, int eflags)
{
	char			*svr_address = NULL;
	int			svr_port = 0;
	char			*svr_username = NULL;
	char			*svr_password = NULL;
	char			*svr_database = NULL;
	char			*svr_schema = NULL;
	char			*svr_table = NULL;
	MYSQL			*conn;
	char			*qual_key = NULL;
	char			*qual_value = NULL;
	bool			pushdown = false;
	MySQLFdwExecutionState  *festate;
	char			query[1024];

#ifdef DEBUG
	elog(NOTICE, "BeginForeignScan");
#endif

	/* Fetch options  */
	mysqlGetOptions(RelationGetRelid(node->ss.ss_currentRelation), &svr_address, &svr_port, &svr_username, &svr_password, &svr_database, &svr_schema, &svr_table);

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

	/* See if we've got a qual we can push down */
	if (node->ss.ps.plan->qual)
	{
		ListCell	*lc;

		foreach (lc, node->ss.ps.qual)
		{
			/* Only the first qual can be pushed down to MySQL */
			ExprState  *state = lfirst(lc);

			mysqlGetQual((Node *) state->expr, node->ss.ss_currentRelation->rd_att, &qual_key, &qual_value, &pushdown);
			if (pushdown)
				break;
		}
	}

	/* Stash away the state info we have already */
	festate = (MySQLFdwExecutionState *) palloc(sizeof(MySQLFdwExecutionState));
	node->fdw_state = (void *) festate;
	festate->conn = conn;
	festate->row = 0;
	festate->address = svr_address;
	festate->port = svr_port;

	/* OK, we connected. If this is an EXPLAIN, bail out now */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Execute the query */
	snprintf(query, sizeof(query), "SELECT * FROM `%s`.`%s`", svr_schema, svr_table);
	if (mysql_query(conn, query) != 0)
	{
                ereport(ERROR,
                        (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
                        errmsg("failed to execute the MySQL query: %s", mysql_error(conn))
                        ));
	}

	/* Store the additional state info */
	festate->attinmeta = TupleDescGetAttInMetadata(node->ss.ss_currentRelation->rd_att);
}

/*
 * mysqlIterateForeignScan
 *		Read next record from the data file and store it into the
 *		ScanTupleSlot as a virtual tuple
 */
static TupleTableSlot *
mysqlIterateForeignScan(ForeignScanState *node)
{
	bool			found = false;
	char			*key;
	char 			*data = 0;
	char			**values;
	HeapTuple		tuple;

	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

#ifdef DEBUG
	elog(NOTICE, "mysqlIterateForeignScan");
#endif

	/* Cleanup */
	ExecClearTuple(slot);


	/* Get the next tuple */
	if (festate->row < 10)
		found = true;
	festate->row++;

	/* Build the tuple */
	values = (char **) palloc(sizeof(char *) * 2);

	if (found)
	{
		values[0] = "<key>";
		values[1] = "<vaue>";
		tuple = BuildTupleFromCStrings(festate->attinmeta, values);
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	}

	/* Cleanup */

	return slot;
}

/*
 * mysqlEndForeignScan
 *		Finish scanning foreign table and dispose objects used for this scan
 */
static void
mysqlEndForeignScan(ForeignScanState *node)
{
	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "mysqlEndForeignScan");
#endif

	mysql_close(festate->conn);
	festate->conn = NULL;
}

/*
 * mysqlReScanForeignScan
 *		Rescan table, possibly with new parameters
 */
static void
mysqlReScanForeignScan(ForeignScanState *node)
{
	MySQLFdwExecutionState *festate = (MySQLFdwExecutionState *) node->fdw_state;

#ifdef DEBUG
	elog(NOTICE, "mysqlReScanForeignScan");
#endif

	festate->row = 0;
}

static void
mysqlGetQual(Node *node, TupleDesc tupdesc, char **key, char **value, bool *pushdown)
{
	*key = NULL;
	*value = NULL;
	*pushdown = false;

	if (!node)
		return;

	if (IsA(node, OpExpr))
	{
		OpExpr	*op = (OpExpr *) node;
		Node	*left, *right;
		Index	varattno;

		if (list_length(op->args) != 2)
			return;

		left = list_nth(op->args, 0);

		if (!IsA(left, Var))
			return;

		varattno = ((Var *) left)->varattno;

		right = list_nth(op->args, 1);

		if (IsA(right, Const))
		{
			StringInfoData  buf;

			initStringInfo(&buf);

			/* And get the column and value... */
			*key = NameStr(tupdesc->attrs[varattno - 1]->attname);
			*value = TextDatumGetCString(((Const *) right)->constvalue);

			/*
			 * We can push down this qual if:
			 * - The operatory is TEXTEQ
			 * - The qual is on the key column
			 */
			if (op->opfuncid == PROCID_TEXTEQ && strcmp(*key, "key") == 0)
                        	*pushdown = true;

			elog(NOTICE, "Got qual %s = %s", *key, *value);
			return;
		}
	}

	return;
}
