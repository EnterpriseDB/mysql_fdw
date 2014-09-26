/*-------------------------------------------------------------------------
 *
 * mysql_fdw.h
 *		  Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 *		  mysql_fdw.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef MYSQL_FDW_H
#define MYSQL_FDW_H

#define list_length mysql_list_length
#define list_delete mysql_list_delete
#define list_free mysql_list_free

#include <mysql.h>
#undef list_length
#undef list_delete
#undef list_free

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/relation.h"
#include "utils/rel.h"

#define MYSQL_PREFETCH_ROWS	10
#define MYSQL_BLKSIZ		(1024 * 4)
#define MYSQL_PORT		3306

/*
 * Options structure to store the MySQL
 * server information
 */
typedef struct mysql_opt
{
	int     svr_port;		/* MySQL port number */
	char	*svr_address;		/* MySQL server ip address */
	char	*svr_username;		/* MySQL user name */
	char	*svr_password;		/* MySQL password */
	char 	*svr_database;		/* MySQL database name */
	char 	*svr_table;			/* MySQL table name */
} mysql_opt;

/* option.c headers */
extern bool mysql_is_valid_option(const char *option, Oid context);
extern mysql_opt *mysql_get_options(Oid foreigntableid);

#endif   /* MYSQL_FDW_H */

