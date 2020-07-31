/*-------------------------------------------------------------------------
 *
 * mysql_fdw.h
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_fdw.h
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

#include "access/tupdesc.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#else
#include "nodes/pathnodes.h"
#endif
#include "utils/rel.h"

#define MYSQL_PREFETCH_ROWS	100
#define MYSQL_BLKSIZ		(1024 * 4)
#define MYSQL_PORT			3306
#define MAXDATALEN			1024 * 64

#define WAIT_TIMEOUT		0
#define INTERACTIVE_TIMEOUT 0

#define CR_NO_ERROR 0

#define mysql_options (*_mysql_options)
#define mysql_stmt_prepare (*_mysql_stmt_prepare)
#define mysql_stmt_execute (*_mysql_stmt_execute)
#define mysql_stmt_fetch (*_mysql_stmt_fetch)
#define mysql_query (*_mysql_query)
#define mysql_stmt_attr_set (*_mysql_stmt_attr_set)
#define mysql_stmt_close (*_mysql_stmt_close)
#define mysql_stmt_reset (*_mysql_stmt_reset)
#define mysql_free_result (*_mysql_free_result)
#define mysql_stmt_bind_param (*_mysql_stmt_bind_param)
#define mysql_stmt_bind_result (*_mysql_stmt_bind_result)
#define mysql_stmt_init (*_mysql_stmt_init)
#define mysql_stmt_result_metadata (*_mysql_stmt_result_metadata)
#define mysql_stmt_store_result (*_mysql_stmt_store_result)
#define mysql_fetch_row (*_mysql_fetch_row)
#define mysql_fetch_field (*_mysql_fetch_field)
#define mysql_fetch_fields (*_mysql_fetch_fields)
#define mysql_error (*_mysql_error)
#define mysql_close (*_mysql_close)
#define mysql_store_result (*_mysql_store_result)
#define mysql_init (*_mysql_init)
#define mysql_ssl_set (*_mysql_ssl_set)
#define mysql_real_connect (*_mysql_real_connect)
#define mysql_get_host_info (*_mysql_get_host_info)
#define mysql_get_server_info (*_mysql_get_server_info)
#define mysql_get_proto_info (*_mysql_get_proto_info)
#define mysql_stmt_errno (*_mysql_stmt_errno)
#define mysql_errno (*_mysql_errno)
#define mysql_num_fields (*_mysql_num_fields)
#define mysql_num_rows (*_mysql_num_rows)

/*
 * Options structure to store the MySQL
 * server information
 */
typedef struct mysql_opt
{
	int			svr_port;		/* MySQL port number */
	char	   *svr_address;	/* MySQL server ip address */
	char	   *svr_username;	/* MySQL user name */
	char	   *svr_password;	/* MySQL password */
	char	   *svr_database;	/* MySQL database name */
	char	   *svr_table;		/* MySQL table name */
	bool		svr_sa;			/* MySQL secure authentication */
	char	   *svr_init_command;	/* MySQL SQL statement to execute when
									 * connecting to the MySQL server. */
	unsigned long max_blob_size;	/* Max blob size to read without
									 * truncation */
	bool		use_remote_estimate;	/* use remote estimate for rows */

	/* SSL parameters; unused options may be given as NULL */
	char	   *ssl_key;		/* MySQL SSL: path to the key file */
	char	   *ssl_cert;		/* MySQL SSL: path to the certificate file */
	char	   *ssl_ca;			/* MySQL SSL: path to the certificate
								 * authority file */
	char	   *ssl_capath;		/* MySQL SSL: path to a directory that
								 * contains trusted SSL CA certificates in PEM
								 * format */
	char	   *ssl_cipher;		/* MySQL SSL: list of permissible ciphers to
								 * use for SSL encryption */
} mysql_opt;

typedef struct mysql_column
{
	Datum		value;
	unsigned long length;
	bool		is_null;
	bool		error;
	MYSQL_BIND *mysql_bind;
} mysql_column;

typedef struct mysql_table
{
	MYSQL_RES  *mysql_res;
	MYSQL_FIELD *mysql_fields;
	mysql_column *column;
	MYSQL_BIND *mysql_bind;
} mysql_table;

/*
 * FDW-specific information for ForeignScanState
 * fdw_state.
 */
typedef struct MySQLFdwExecState
{
	MYSQL	   *conn;			/* MySQL connection handle */
	MYSQL_STMT *stmt;			/* MySQL prepared stament handle */
	mysql_table *table;
	char	   *query;			/* Query string */
	Relation	rel;			/* relcache entry for the foreign table */
	List	   *retrieved_attrs;	/* list of target attribute numbers */
	bool		cursor_exists;	/* have we created the cursor? */
	int			numParams;		/* number of parameters passed to query */
	FmgrInfo   *param_flinfo;	/* output conversion functions for them */
	List	   *param_exprs;	/* executable expressions for param values */
	const char **param_values;	/* textual values of query parameters */
	Oid		   *param_types;	/* type of query parameters */
	int			p_nums;			/* number of parameters to transmit */
	FmgrInfo   *p_flinfo;		/* output conversion functions for them */

	mysql_opt  *mysqlFdwOptions;	/* MySQL FDW options */

	List	   *attr_list;		/* query attribute list */
	List	   *column_list;	/* Column list of MySQL Column structures */
	/* working memory context */
	MemoryContext temp_cxt;		/* context for per-tuple temporary data */
} MySQLFdwExecState;


/* MySQL Column List */
typedef struct MySQLColumn
{
	int			attnum;			/* Attribute number */
	char	   *attname;		/* Attribute name */
	int			atttype;		/* Attribute type */
} MySQLColumn;

extern bool is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel,
							Expr *expr);

extern int ((mysql_options) (MYSQL *mysql, enum mysql_option option,
							 const void *arg));
extern int ((mysql_stmt_prepare) (MYSQL_STMT *stmt, const char *query,
								  unsigned long length));
extern int ((mysql_stmt_execute) (MYSQL_STMT *stmt));
extern int ((mysql_stmt_fetch) (MYSQL_STMT *stmt));
extern int ((mysql_query) (MYSQL *mysql, const char *q));
extern bool ((mysql_stmt_attr_set) (MYSQL_STMT *stmt,
									enum enum_stmt_attr_type attr_type,
									const void *attr));
extern bool ((mysql_stmt_close) (MYSQL_STMT *stmt));
extern bool ((mysql_stmt_reset) (MYSQL_STMT *stmt));
extern bool ((mysql_free_result) (MYSQL_RES *result));
extern bool ((mysql_stmt_bind_param) (MYSQL_STMT *stmt, MYSQL_BIND *bnd));
extern bool ((mysql_stmt_bind_result) (MYSQL_STMT *stmt, MYSQL_BIND *bnd));

extern MYSQL_STMT *((mysql_stmt_init) (MYSQL *mysql));
extern MYSQL_RES *((mysql_stmt_result_metadata) (MYSQL_STMT *stmt));
extern int ((mysql_stmt_store_result) (MYSQL *mysql));
extern MYSQL_ROW((mysql_fetch_row) (MYSQL_RES *result));
extern MYSQL_FIELD *((mysql_fetch_field) (MYSQL_RES *result));
extern MYSQL_FIELD *((mysql_fetch_fields) (MYSQL_RES *result));
extern const char *((mysql_error) (MYSQL *mysql));
extern void ((mysql_close) (MYSQL *sock));
extern MYSQL_RES *((mysql_store_result) (MYSQL *mysql));
extern MYSQL *((mysql_init) (MYSQL *mysql));
extern bool ((mysql_ssl_set) (MYSQL *mysql, const char *key, const char *cert,
							  const char *ca, const char *capath,
							  const char *cipher));
extern MYSQL *((mysql_real_connect) (MYSQL *mysql, const char *host,
									 const char *user, const char *passwd,
									 const char *db, unsigned int port,
									 const char *unix_socket,
									 unsigned long clientflag));

extern const char *((mysql_get_host_info) (MYSQL *mysql));
extern const char *((mysql_get_server_info) (MYSQL *mysql));
extern int ((mysql_get_proto_info) (MYSQL *mysql));

extern unsigned int ((mysql_stmt_errno) (MYSQL_STMT *stmt));
extern unsigned int ((mysql_errno) (MYSQL *mysql));
extern unsigned int ((mysql_num_fields) (MYSQL_RES *result));
extern unsigned int ((mysql_num_rows) (MYSQL_RES *result));


/* option.c headers */
extern bool mysql_is_valid_option(const char *option, Oid context);
extern mysql_opt *mysql_get_options(Oid foreigntableid);

/* depare.c headers */
extern void mysql_deparse_select(StringInfo buf, PlannerInfo *root,
								 RelOptInfo *baserel, Bitmapset *attrs_used,
								 char *svr_table, List **retrieved_attrs);
extern void mysql_deparse_insert(StringInfo buf, PlannerInfo *root,
								 Index rtindex, Relation rel,
								 List *targetAttrs);
extern void mysql_deparse_update(StringInfo buf, PlannerInfo *root,
								 Index rtindex, Relation rel,
								 List *targetAttrs, char *attname);
extern void mysql_deparse_delete(StringInfo buf, PlannerInfo *root,
								 Index rtindex, Relation rel, char *name);
extern void mysql_append_where_clause(StringInfo buf, PlannerInfo *root,
									  RelOptInfo *baserel, List *exprs,
									  bool is_first, List **params);
extern void mysql_deparse_analyze(StringInfo buf, char *dbname, char *relname);


/* connection.c headers */
MYSQL *mysql_get_connection(ForeignServer *server, UserMapping *user,
							mysql_opt *opt);
MYSQL *mysql_connect(mysql_opt *opt);
void mysql_cleanup_connection(void);
void mysql_release_connection(MYSQL *conn);

#if PG_VERSION_NUM < 110000		/* TupleDescAttr is defined from PG version 11 */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif

#endif							/* MYSQL_FDW_H */
