/*-------------------------------------------------------------------------
 *
 * mysql_query.h
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_query.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MYSQL_QUERY_H
#define MYSQL_QUERY_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#else
#include "nodes/pathnodes.h"
#endif
#include "utils/rel.h"


Datum mysql_convert_to_pg(Oid pgtyp, int pgtypmod, mysql_column *column);
void mysql_bind_sql_var(Oid type, int attnum, Datum value, MYSQL_BIND *binds,
						bool *isnull);
void mysql_bind_result(Oid pgtyp, int pgtypmod, MYSQL_FIELD *field,
					   mysql_column *column);

#endif							/* MYSQL_QUERY_H */
