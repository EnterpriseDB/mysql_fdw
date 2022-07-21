/*-------------------------------------------------------------------------
 *
 * option.c
 * 		FDW option handling for mysql_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "mysql_fdw.h"
#include "utils/lsyscache.h"

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct MySQLFdwOption
{
	const char *optname;
	Oid			optcontext;		/* Oid of catalog in which option may appear */
};

/*
 * Valid options for mysql_fdw.
 */
static struct MySQLFdwOption valid_options[] =
{
	/* Connection options */
	{"host", ForeignServerRelationId},
	{"port", ForeignServerRelationId},
	{"init_command", ForeignServerRelationId},
	{"username", UserMappingRelationId},
	{"password", UserMappingRelationId},
	{"dbname", ForeignTableRelationId},
	{"table_name", ForeignTableRelationId},
	{"secure_auth", ForeignServerRelationId},
	{"max_blob_size", ForeignTableRelationId},
	{"use_remote_estimate", ForeignServerRelationId},
	/* fetch_size is available on both server and table */
	{"fetch_size", ForeignServerRelationId},
	{"fetch_size", ForeignTableRelationId},
	{"reconnect", ForeignServerRelationId},
	{"character_set", ForeignServerRelationId},
#if PG_VERSION_NUM >= 140000
	/* truncatable is available on both server and table */
	{"truncatable", ForeignServerRelationId},
	{"truncatable", ForeignTableRelationId},
#endif
	{"sql_mode", ForeignServerRelationId},
	{"ssl_key", ForeignServerRelationId},
	{"ssl_cert", ForeignServerRelationId},
	{"ssl_ca", ForeignServerRelationId},
	{"ssl_capath", ForeignServerRelationId},
	{"ssl_cipher", ForeignServerRelationId},

	/* Sentinel */
	{NULL, InvalidOid}
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
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	/*
	 * Check that only options supported by mysql_fdw, and allowed for the
	 * current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

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
					 errhint("Valid options in this context are: %s",
							 buf.len ? buf.data : "<none>")));
		}

		/* Validate fetch_size option value */
		if (strcmp(def->defname, "fetch_size") == 0)
		{
			unsigned long fetch_size;
			char	   *endptr;
			char	   *inputVal = defGetString(def);

			while (inputVal && isspace((unsigned char) *inputVal))
				inputVal++;

			if (inputVal && *inputVal == '-')
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"%s\" requires an integer value between 1 to %lu",
								def->defname, ULONG_MAX)));

			errno = 0;
			fetch_size = strtoul(inputVal, &endptr, 10);

			if (*endptr != '\0' ||
				(errno == ERANGE && fetch_size == ULONG_MAX) ||
				fetch_size == 0)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("\"%s\" requires an integer value between 1 to %lu",
								def->defname, ULONG_MAX)));
		}
		else if (strcmp(def->defname, "reconnect") == 0)
		{
			/* accept only boolean values */
			(void) defGetBoolean(def);
		}
#if PG_VERSION_NUM >= 140000
		else if (strcmp(def->defname, "truncatable") == 0)
		{
			/* accept only boolean values */
			(void) defGetBoolean(def);
		}
#endif
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
mysql_opt *
mysql_get_options(Oid foreignoid, bool is_foreigntable)
{
	ForeignTable *f_table;
	ForeignServer *f_server;
	UserMapping *f_mapping;
	List	   *options;
	ListCell   *lc;
	mysql_opt  *opt;

	opt = (mysql_opt *) palloc0(sizeof(mysql_opt));

	/*
	 * Extract options from FDW objects.
	 */
	if (is_foreigntable)
	{
		f_table = GetForeignTable(foreignoid);
		f_server = GetForeignServer(f_table->serverid);
	}
	else
	{
		f_table = NULL;
		f_server = GetForeignServer(foreignoid);
	}

	f_mapping = GetUserMapping(GetUserId(), f_server->serverid);

	options = NIL;

	options = mysql_list_concat(options, f_server->options);
	options = mysql_list_concat(options, f_mapping->options);

	if (f_table)
		options = mysql_list_concat(options, f_table->options);

	/* Default secure authentication is true */
	opt->svr_sa = true;

	opt->use_remote_estimate = false;
	opt->reconnect = false;

	/* Loop through the options */
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

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

		if (strcmp(def->defname, "fetch_size") == 0)
			opt->fetch_size = strtoul(defGetString(def), NULL, 10);

		if (strcmp(def->defname, "reconnect") == 0)
			opt->reconnect = defGetBoolean(def);

		if (strcmp(def->defname, "character_set") == 0)
			opt->character_set = defGetString(def);

		if (strcmp(def->defname, "sql_mode") == 0)
			opt->sql_mode = defGetString(def);

		if (strcmp(def->defname, "ssl_key") == 0)
			opt->ssl_key = defGetString(def);

		if (strcmp(def->defname, "ssl_cert") == 0)
			opt->ssl_cert = defGetString(def);

		if (strcmp(def->defname, "ssl_ca") == 0)
			opt->ssl_ca = defGetString(def);

		if (strcmp(def->defname, "ssl_capath") == 0)
			opt->ssl_capath = defGetString(def);

		if (strcmp(def->defname, "ssl_cipher") == 0)
			opt->ssl_cipher = defGetString(def);
	}

	/* Default values, if required */
	if (!opt->svr_address)
		opt->svr_address = "127.0.0.1";

	if (!opt->svr_port)
		opt->svr_port = MYSQL_SERVER_PORT;

	/*
	 * When we don't have a table name or database name provided in the
	 * FOREIGN TABLE options, then use a foreign table name as the target
	 * table name and the namespace of the foreign table as a database name.
	 */
	if (f_table)
	{
		if (!opt->svr_table)
			opt->svr_table = get_rel_name(foreignoid);

		if (!opt->svr_database)
			opt->svr_database = get_namespace_name(get_rel_namespace(foreignoid));
	}

	/* Default value for fetch_size */
	if (!opt->fetch_size)
		opt->fetch_size = MYSQL_PREFETCH_ROWS;

	/* Default value for character_set */
	if (!opt->character_set)
		opt->character_set = MYSQL_AUTODETECT_CHARSET_NAME;
	/* Special value provided for existing behavior */
	else if (strcmp(opt->character_set, "PGDatabaseEncoding") == 0)
		opt->character_set = (char *) GetDatabaseEncodingName();

	/* Default value for sql_mode */
	if (!opt->sql_mode)
		opt->sql_mode = "ANSI_QUOTES";

	return opt;
}
