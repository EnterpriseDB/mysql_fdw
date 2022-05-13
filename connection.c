/*-------------------------------------------------------------------------
 *
 * connection.c
 * 		Connection management functions for mysql_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		connection.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#endif
#include "mysql_fdw.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

/* Length of host */
#define HOST_LEN 256

/*
 * Connection cache hash table entry
 *
 * The lookup key in this hash table is the foreign server OID plus the user
 * mapping OID.  (We use just one connection per user per foreign server,
 * so that we can ensure all scans use the same snapshot during a query.)
 */
typedef struct ConnCacheKey
{
	Oid			serverid;		/* OID of foreign server */
	Oid			userid;			/* OID of local user whose mapping we use */
} ConnCacheKey;

typedef struct ConnCacheEntry
{
	ConnCacheKey key;			/* hash key (must be first) */
	MYSQL	   *conn;			/* connection to foreign server, or NULL */
	bool		invalidated;	/* true if reconnect is pending */
	uint32		server_hashvalue;	/* hash value of foreign server OID */
	uint32		mapping_hashvalue;	/* hash value of user mapping OID */
} ConnCacheEntry;

/*
 * Connection cache (initialized on first use)
 */
static HTAB *ConnectionHash = NULL;

static void mysql_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

/*
 * mysql_get_connection:
 * 		Get a connection which can be used to execute queries on the remote
 * 		MySQL server with the user's authorization.  A new connection is
 * 		established if we don't already have a suitable one.
 */
MYSQL *
mysql_get_connection(ForeignServer *server, UserMapping *user, mysql_opt *opt)
{
	bool		found;
	ConnCacheEntry *entry;
	ConnCacheKey key;

	/* First time through, initialize connection cache hashtable */
	if (ConnectionHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(ConnCacheKey);
		ctl.entrysize = sizeof(ConnCacheEntry);
		ctl.hash = tag_hash;

		/* Allocate ConnectionHash in the cache context */
		ctl.hcxt = CacheMemoryContext;
		ConnectionHash = hash_create("mysql_fdw connections", 8,
									 &ctl,
									 HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

		/*
		 * Register some callback functions that manage connection cleanup.
		 * This should be done just once in each backend.
		 */
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  mysql_inval_callback, (Datum) 0);
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
									  mysql_inval_callback, (Datum) 0);
	}

	/* Create hash key for the entry.  Assume no pad bytes in key struct */
	key.serverid = server->serverid;
	key.userid = user->userid;

	/*
	 * Find or create cached entry for requested connection.
	 */
	entry = hash_search(ConnectionHash, &key, HASH_ENTER, &found);
	if (!found)
	{
		/* Initialize new hashtable entry (key is already filled in) */
		entry->conn = NULL;
	}

	/* If an existing entry has invalid connection then release it */
	if (entry->conn != NULL && entry->invalidated)
	{
		elog(DEBUG3, "disconnecting mysql_fdw connection %p for option changes to take effect",
			 entry->conn);
		mysql_close(entry->conn);
		entry->conn = NULL;
	}

	if (entry->conn == NULL)
	{
		entry->conn = mysql_connect(opt);
		elog(DEBUG3, "new mysql_fdw connection %p for server \"%s\"",
			 entry->conn, server->servername);

		/*
		 * Once the connection is established, then set the connection
		 * invalidation flag to false, also set the server and user mapping
		 * hash values.
		 */
		entry->invalidated = false;
		entry->server_hashvalue =
			GetSysCacheHashValue1(FOREIGNSERVEROID,
								  ObjectIdGetDatum(server->serverid));

		entry->mapping_hashvalue =
			GetSysCacheHashValue1(USERMAPPINGOID,
								  ObjectIdGetDatum(user->umid));
	}
	return entry->conn;
}

/*
 * mysql_cleanup_connection:
 * 		Delete all the cache entries on backend exists.
 */
void
mysql_cleanup_connection(void)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		elog(DEBUG3, "disconnecting mysql_fdw connection %p", entry->conn);
		mysql_close(entry->conn);
		entry->conn = NULL;
	}
}

/*
 * Release connection created by calling mysql_get_connection.
 */
void
mysql_release_connection(MYSQL *conn)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	if (ConnectionHash == NULL)
		return;

	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		if (entry->conn == NULL)
			continue;

		if (entry->conn == conn)
		{
			elog(DEBUG3, "disconnecting mysql_fdw connection %p", entry->conn);
			mysql_close(entry->conn);
			entry->conn = NULL;
			hash_seq_term(&scan);
			break;
		}
	}
}

MYSQL *
mysql_connect(mysql_opt *opt)
{
	MYSQL	   *conn;
	char	   *svr_database = opt->svr_database;
	bool		svr_sa = opt->svr_sa;
	char	   *svr_init_command = opt->svr_init_command;
	char	   *ssl_cipher = opt->ssl_cipher;
#if	MYSQL_VERSION_ID < 80000
	my_bool		secure_auth = svr_sa;
#endif

	/* Connect to the server */
	conn = mysql_init(NULL);
	if (!conn)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("failed to initialise the MySQL connection object")));

	mysql_options(conn, MYSQL_SET_CHARSET_NAME, opt->character_set);
#if MYSQL_VERSION_ID < 80000
	mysql_options(conn, MYSQL_SECURE_AUTH, &secure_auth);
#endif

	if (!svr_sa)
		elog(WARNING, "MySQL secure authentication is off");

	if (svr_init_command != NULL)
		mysql_options(conn, MYSQL_INIT_COMMAND, svr_init_command);

	/*
	 * Enable or disable automatic reconnection to the MySQL server if the
	 * existing connection is found to have been lost.
	 */
	mysql_options(conn, MYSQL_OPT_RECONNECT, &opt->reconnect);

	mysql_ssl_set(conn, opt->ssl_key, opt->ssl_cert, opt->ssl_ca,
				  opt->ssl_capath, ssl_cipher);

	if (!mysql_real_connect(conn, opt->svr_address, opt->svr_username,
							opt->svr_password, svr_database, opt->svr_port,
							NULL, 0))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("failed to connect to MySQL: %s", mysql_error(conn))));

	/* Useful for verifying that the connection's secured */
	elog(DEBUG1,
		 "Successfully connected to MySQL database %s at server %s with cipher %s (server version: %s, protocol version: %d) ",
		 (svr_database != NULL) ? svr_database : "<none>",
		 mysql_get_host_info(conn),
		 (ssl_cipher != NULL) ? ssl_cipher : "<none>",
		 mysql_get_server_info(conn),
		 mysql_get_proto_info(conn));

	return conn;
}

/*
 * Connection invalidation callback function for mysql.
 *
 * After a change to a pg_foreign_server or pg_user_mapping catalog entry,
 * mark connections depending on that entry as needing to be remade. This
 * implementation is similar as pgfdw_inval_callback.
 */
static void
mysql_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS scan;
	ConnCacheEntry *entry;

	Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

	/* ConnectionHash must exist already, if we're registered */
	hash_seq_init(&scan, ConnectionHash);
	while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
	{
		/* Ignore invalid entries */
		if (entry->conn == NULL)
			continue;

		/* hashvalue == 0 means a cache reset, must clear all state */
		if (hashvalue == 0 ||
			(cacheid == FOREIGNSERVEROID &&
			 entry->server_hashvalue == hashvalue) ||
			(cacheid == USERMAPPINGOID &&
			 entry->mapping_hashvalue == hashvalue))
			entry->invalidated = true;
	}
}
