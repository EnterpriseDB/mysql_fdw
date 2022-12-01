/*-------------------------------------------------------------------------
 *
 * mysql_pushability.c
 *		routines for FDW pushability
 *
 * Portions Copyright (c) 2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 *		mysql_pushability.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/string.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "mysql_pushability.h"
#include "storage/fd.h"
#include "utils/fmgrprotos.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

static char *get_config_filename(void);
static void populate_pushability_hash(void);
static void config_invalid_error_callback(void *arg);
static bool get_line_buf(FILE *stream, StringInfo buf);

/* Hash table for caching the configured pushdown objects */
static HTAB *pushabilityHash = NULL;

/*
 * Memory context to hold the hash table, need to free incase of any error
 * while parsing the configuration file.
 */
static MemoryContext htab_ctx;


/*
 * get_config_filename
 * 		Returns the path for the pushdown object configuration file for the
 * 		foreign-data wrapper.
 */
static char *
get_config_filename(void)
{
	char		sharepath[MAXPGPATH];
	char	   *result;

	get_share_path(my_exec_path, sharepath);
	result = (char *) palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/extension/%s_pushdown.config", sharepath,
			 FDW_MODULE_NAME);

	return result;
}

/*
 * mysql_check_remote_pushability
 * 		Lookups into hash table by forming the hash key from provided object
 * 		oid.
 */
bool
mysql_check_remote_pushability(Oid object_oid)
{
	bool		found = false;

	/* Populate pushability hash if not already. */
	if (unlikely(!pushabilityHash))
		populate_pushability_hash();

	hash_search(pushabilityHash, &object_oid, HASH_FIND, &found);

	return found;
}

/*
 * populate_pushability_hash
 * 		Creates the hash table and populates the hash entries by reading the
 * 		pushdown object configuration file.
 */
static void
populate_pushability_hash(void)
{
	FILE	   *file = NULL;
	char	   *config_filename;
	HASHCTL		ctl;
	ErrorContextCallback errcallback;
	unsigned int line_no = 0;
	StringInfoData linebuf;
	HTAB	   *hash;

	Assert(pushabilityHash == NULL);

	/*
	 * Create a memory context to hold hash table.  This makes it easy to
	 * clean up in the case of error, we don't make the context long-lived
	 * until we parse the complete config file without an error.
	 */
	htab_ctx = AllocSetContextCreate(CurrentMemoryContext,
									 "mysql pushability_hash",
									 ALLOCSET_DEFAULT_SIZES);
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(FDWPushdownObject);
	ctl.hcxt = htab_ctx;

	/* Create the hash table */
	hash = hash_create("mysql_fdw push elements hash", 256,
					   &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/* Get the config file name */
	config_filename = get_config_filename();

	file = AllocateFile(config_filename, PG_BINARY_R);

	if (file == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open \"%s\": %m", config_filename)));

	/* Set up callback to provide the error context */
	errcallback.callback = config_invalid_error_callback;
	errcallback.arg = (void *) config_filename;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	initStringInfo(&linebuf);

	/*
	 * Read the pushdown object configuration file and push object information
	 * to the in-memory hash table for a faster lookup.
	 */
	while (get_line_buf(file, &linebuf))
	{
		FDWPushdownObject *entry;
		Oid			objectId;
		ObjectType	objectType;
		bool		found;
		char	   *str;

		line_no++;

		/* If record starts with #, then consider as comment. */
		if (linebuf.data[0] == '#')
			continue;

		/* Ignore if all blank */
		if (strspn(linebuf.data, " \t\r\n") == linebuf.len)
			continue;

		/* Strip trailing newline, including \r in case we're on Windows */
		while (linebuf.len > 0 && (linebuf.data[linebuf.len - 1] == '\n' ||
								   linebuf.data[linebuf.len - 1] == '\r'))
			linebuf.data[--linebuf.len] = '\0';

		/* Strip leading whitespaces. */
		str = linebuf.data;
		while (isspace(*str))
			str++;

		if (pg_strncasecmp(str, "ROUTINE", 7) == 0)
		{
			/* Move over ROUTINE */
			str = str + 7;

			/* Move over any whitespace  */
			while (isspace(*str))
				str++;

			objectType = OBJECT_FUNCTION;
			objectId =
				DatumGetObjectId(DirectFunctionCall1(regprocedurein,
													 CStringGetDatum(str)));
		}
		else if (pg_strncasecmp(str, "OPERATOR", 8) == 0)
		{
			/* Move over OPERATOR */
			str = str + 8;

			/* Move over any whitespace  */
			while (isspace(*str))
				str++;

			objectType = OBJECT_OPERATOR;
			objectId =
				DatumGetObjectId(DirectFunctionCall1(regoperatorin,
													 CStringGetDatum(str)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid object type in configuration file at line number: %d",
							line_no),
					 errhint("Valid values are: \"ROUTINE\", \"OPERATOR\".")));

		/* Insert the new element to the hash table */
		entry = hash_search(hash, &objectId, HASH_ENTER, &found);

		/* Two different objects cannot have the same system object id */
		if (found && entry->objectType != objectType)
			elog(ERROR, "different pushdown objects have the same oid \"%d\"",
				 objectId);

		entry->objectType = objectType;
	}

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", config_filename)));

	error_context_stack = errcallback.previous;

	pfree(linebuf.data);

	FreeFile(file);

	/*
	 * We have fully parsed the config file.  Reparent hash table context so
	 * that it has the right lifespan.
	 */
	MemoryContextSetParent(htab_ctx, CacheMemoryContext);
	pushabilityHash = hash;
}

/*
 * config_invalid_error_callback
 * 		Error callback to define the context.
 */
static void
config_invalid_error_callback(void *arg)
{
	char	   *filename = (char *) arg;

	/* Destroy the hash in case of error */
	hash_destroy(pushabilityHash);
	pushabilityHash = NULL;

	errcontext("while processing \"%s\" file", filename);
}

/*
 * get_line_buf
 * 		Returns true if a line was successfully collected (including
 * 		the case of a non-newline-terminated line at EOF).
 *
 * Returns false if there was an I/O error or no data was available
 * before EOF. In the false-result case, buf is reset to empty.
 * (Borrowed the code from pg_get_line_buf().)
 */
bool
get_line_buf(FILE *stream, StringInfo buf)
{
	int			orig_len;

	/* We just need to drop any data from the previous call */
	resetStringInfo(buf);

	orig_len = buf->len;

	/* Read some data, appending it to whatever we already have */
	while (fgets(buf->data + buf->len, buf->maxlen - buf->len, stream) != NULL)
	{
		buf->len += strlen(buf->data + buf->len);

		/* Done if we have collected a newline */
		if (buf->len > orig_len && buf->data[buf->len - 1] == '\n')
			return true;

		/* Make some more room in the buffer, and loop to read more data */
		enlargeStringInfo(buf, 128);
	}

	/* Check for I/O errors and EOF */
	if (ferror(stream) || buf->len == orig_len)
	{
		/* Discard any data we collected before detecting error */
		buf->len = orig_len;
		buf->data[orig_len] = '\0';
		return false;
	}

	/* No newline at EOF, but we did collect some data */
	return true;
}

/*
 * mysql_get_configured_pushdown_objects
 * 		Returns the hash table objects by sequentially scanning the hash table.
 */
List *
mysql_get_configured_pushdown_objects(bool reload)
{
	List	   *result = NIL;
	HASH_SEQ_STATUS scan;
	FDWPushdownObject *entry;
	FDWPushdownObject *object;
	Size		size = sizeof(FDWPushdownObject);

	/*
	 * To avoid the memory leak, destroy the existing hash in case of
	 * reloading.
	 */
	if (reload)
	{
		hash_destroy(pushabilityHash);
		pushabilityHash = NULL;
		MemoryContextDelete(htab_ctx);
	}

	/* Reload configuration if that not loaded at all */
	if (!pushabilityHash)
		populate_pushability_hash();

	hash_seq_init(&scan, pushabilityHash);
	while ((entry = (FDWPushdownObject *) hash_seq_search(&scan)) != NULL)
	{
		object = (FDWPushdownObject *) palloc(size);
		memcpy(object, entry, size);
		result = lappend(result, object);
	}

	return result;
}
