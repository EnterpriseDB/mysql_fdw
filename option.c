/*-------------------------------------------------------------------------
 *
 * options.c
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		options.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "mysql_fdw.h"

#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

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
#include "utils/lsyscache.h"

#include "optimizer/pathnode.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/planmain.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MySQLFdwOption
{
	const char *optname;
	Oid optcontext; /* Oid of catalog in which option may appear */
};


/*
 * Valid options for mysql_fdw.
 *
 */
static struct MySQLFdwOption valid_options[] =
{
	/* Connection options */
	{ "host",           ForeignServerRelationId },
	{ "port",           ForeignServerRelationId },
	{ "init_command",   ForeignServerRelationId },
	{ "username",       UserMappingRelationId },
	{ "password",       UserMappingRelationId },
	{ "dbname",         ForeignTableRelationId },
	{ "table_name",     ForeignTableRelationId },
	{ "secure_auth",    ForeignServerRelationId },
	{ "max_blob_size",  ForeignTableRelationId },
	{ "use_remote_estimate",    ForeignServerRelationId },

	/* Sentinel */
	{ NULL,			InvalidOid }
};

extern Datum mysql_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(mysql_fdw_validator);


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
	Oid			catalog = PG_GETARG_OID(1);
	ListCell	*cell;

	/*
	 * Check that only options supported by mysql_fdw,
	 * and allowed for the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem	 *def = (DefElem *) lfirst(cell);

		if (!mysql_is_valid_option(def->defname, catalog))
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
	}
	PG_RETURN_VOID();
}


/*
 * Check if the provided option is one of the valid options.
 * context is the Oid of the catalog holding the object the option is for.
 */
bool
mysql_is_valid_option(const char *option, Oid context)
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
mysql_opt*
mysql_get_options(Oid foreignoid)
{
	ForeignTable *f_table = NULL;
	ForeignServer *f_server = NULL;
	UserMapping *f_mapping;
	List *options;
	ListCell *lc;
	mysql_opt *opt;

	opt = (mysql_opt*) palloc(sizeof(mysql_opt));
	memset(opt, 0, sizeof(mysql_opt));

	/*
	 * Extract options from FDW objects.
	 */
	PG_TRY();
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	PG_CATCH();
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}
	PG_END_TRY();

	f_mapping = GetUserMapping(GetUserId(), f_server->serverid);

	options = NIL;
	if (f_table)
		options = list_concat(options, f_table->options);
	options = list_concat(options, f_server->options);
	options = list_concat(options, f_mapping->options);

	/* Default secure authentication is true */
	opt->svr_sa = true;

	opt->use_remote_estimate = false;

	/* Loop through the options, and get the server/port */
	foreach(lc, options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "host") == 0)
			opt->svr_address = defGetString(def);

		if (strcmp(def->defname, "port") == 0)
			opt->svr_port = atoi(defGetString(def));

		if (strcmp(def->defname, "username") == 0)
			opt->svr_username = defGetString(def);

		if (strcmp(def->defname, "password") == 0)
			opt->svr_password = defGetString(def);

		if (strcmp(def->defname, "dbname") == 0)
			opt->svr_database = defGetString(def);

		if (strcmp(def->defname, "table_name") == 0)
			opt->svr_table = defGetString(def);

		if (strcmp(def->defname, "secure_auth") == 0)
			opt->svr_sa = defGetBoolean(def);
		
		if (strcmp(def->defname, "init_command") == 0)
			opt->svr_init_command = defGetString(def);

		if (strcmp(def->defname, "max_blob_size") == 0)
                       opt->max_blob_size = strtoul(defGetString(def), NULL, 0);

		if (strcmp(def->defname, "use_remote_estimate") == 0)
			opt->use_remote_estimate = defGetBoolean(def);

	}
	/* Default values, if required */
	if (!opt->svr_address)
		opt->svr_address = "127.0.0.1";

	if (!opt->svr_port)
		opt->svr_port = MYSQL_PORT;

	if (!opt->svr_table && f_table)
		opt->svr_table = get_rel_name(foreignoid);

	return opt;
}


