/*-------------------------------------------------------------------------
 *
 * mysql_query.c
 * 		Type handling for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

/*
 * Must be included before mysql.h as it has some conflicting definitions like
 * list_length, etc.
 */
#include "mysql_fdw.h"

#include <mysql.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "mysql_query.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#define DATE_MYSQL_PG(x, y) \
do { \
x->year = y.tm_year; \
x->month = y.tm_mon; \
x->day= y.tm_mday; \
x->hour = y.tm_hour; \
x->minute = y.tm_min; \
x->second = y.tm_sec; \
} while(0);

static int32 mysql_from_pgtyp(Oid type);
static int dec_bin(int number);
static int bin_dec(int binarynumber);


/*
 * convert_mysql_to_pg:
 * 		Convert MySQL data into PostgreSQL's compatible data types
 */
Datum
mysql_convert_to_pg(Oid pgtyp, int pgtypmod, mysql_column *column)
{
	Datum		value_datum;
	Datum		valueDatum;
	regproc		typeinput;
	HeapTuple	tuple;
	char		str[MAXDATELEN];
	bytea	   *result;
	char	   *text_result = NULL;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type%u", pgtyp);

	typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
	ReleaseSysCache(tuple);

	switch (pgtyp)
	{
		/*
		 * MySQL gives BIT / BIT(n) data type as decimal value.  The only way
		 * to retrieve this value is to use BIN, OCT or HEX function in MySQL,
		 * otherwise mysql client shows the actual decimal value, which could
		 * be a non - printable character.  For exmple in MySQL
		 *
		 * CREATE TABLE t (b BIT(8));
		 * INSERT INTO t SET b = b'1001';
		 * SELECT BIN(b) FROM t;
		 * +--------+
		 * | BIN(b) |
		 * +--------+
		 * | 1001   |
		 * +--------+
		 *
		 * PostgreSQL expacts all binary data to be composed of either '0' or
		 * '1'. MySQL gives value 9 hence PostgreSQL reports error.  The
		 * solution is to convert the decimal number into equivalent binary
		 * string.
		 */
		case BYTEAOID:
			result = (bytea *) palloc(column->length + VARHDRSZ);
			memcpy(VARDATA(result), VARDATA(column->value), column->length);
			SET_VARSIZE(result, column->length + VARHDRSZ);
			return PointerGetDatum(result);

		case BITOID:
			sprintf(str, "%d", dec_bin(*((int *) column->value)));
			valueDatum = CStringGetDatum((char *) str);
			break;

		case TEXTOID:
			text_result = (char *) palloc(column->length + 1);
			memcpy(text_result, (char *) column->value, column->length);
			text_result[column->length] = '\0';
			valueDatum = CStringGetDatum((char *) text_result);
			break;

		default:
			valueDatum = CStringGetDatum((char *) column->value);
	}

	value_datum = OidFunctionCall3(typeinput, valueDatum,
								   ObjectIdGetDatum(pgtyp),
								   Int32GetDatum(pgtypmod));

	if (text_result)
		pfree(text_result);

	return value_datum;
}

/*
 * mysql_from_pgtyp:
 * 		Give MySQL data type for PG type
 */
static int32
mysql_from_pgtyp(Oid type)
{
	switch (type)
	{
		case INT2OID:
			return MYSQL_TYPE_SHORT;
		case INT4OID:
			return MYSQL_TYPE_LONG;
		case INT8OID:
			return MYSQL_TYPE_LONGLONG;
		case FLOAT4OID:
			return MYSQL_TYPE_FLOAT;
		case FLOAT8OID:
			return MYSQL_TYPE_DOUBLE;
		case NUMERICOID:
			return MYSQL_TYPE_DOUBLE;
		case BOOLOID:
			return MYSQL_TYPE_LONG;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case ANYENUMOID:
			return MYSQL_TYPE_STRING;
		case NAMEOID:
			return MYSQL_TYPE_STRING;
		case DATEOID:
			return MYSQL_TYPE_DATE;
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return MYSQL_TYPE_TIMESTAMP;
		case BITOID:
			return MYSQL_TYPE_LONG;
		case BYTEAOID:
			return MYSQL_TYPE_BLOB;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot convert constant value to MySQL value"),
					 errhint("Constant value data type: %u", type)));
			break;
	}
}

/*
 * bind_sql_var:
 * 		Bind the values provided as DatumBind the values and nulls to
 * 		modify the target table (INSERT/UPDATE)
 */
void
mysql_bind_sql_var(Oid type, int attnum, Datum value, MYSQL_BIND *binds,
				   bool *isnull)
{
	/* Clear the bind buffer and attributes */
	memset(&binds[attnum], 0x0, sizeof(MYSQL_BIND));

#if MYSQL_VERSION_ID < 80000 || MARIADB_VERSION_ID >= 100000
	binds[attnum].is_null = (my_bool *) isnull;
#else
	binds[attnum].is_null = isnull;
#endif

	/* Avoid to bind buffer in case value is NULL */
	if (*isnull)
		return;

	/*
	 * If type is an enum, use ANYENUMOID.  We will send string containing the
	 * enum value to the MySQL.
	 */
	if (type_is_enum(type))
		type = ANYENUMOID;

	/* Assign the buffer type if value is not null */
	binds[attnum].buffer_type = mysql_from_pgtyp(type);

	switch (type)
	{
		case INT2OID:
			{
				int16		dat = DatumGetInt16(value);
				int16	   *bufptr = palloc(sizeof(int16));

				memcpy(bufptr, (char *) &dat, sizeof(int16));

				binds[attnum].buffer = bufptr;
			}
			break;
		case INT4OID:
			{
				int32		dat = DatumGetInt32(value);
				int32	   *bufptr = palloc(sizeof(int32));

				memcpy(bufptr, (char *) &dat, sizeof(int32));

				binds[attnum].buffer = bufptr;
			}
			break;
		case INT8OID:
			{
				int64		dat = DatumGetInt64(value);
				int64	   *bufptr = palloc(sizeof(int64));

				memcpy(bufptr, (char *) &dat, sizeof(int64));

				binds[attnum].buffer = bufptr;
			}
			break;
		case FLOAT4OID:
			{
				float4		dat = DatumGetFloat4(value);
				float4	   *bufptr = palloc(sizeof(float4));

				memcpy(bufptr, (char *) &dat, sizeof(float4));

				binds[attnum].buffer = bufptr;
			}
			break;
		case FLOAT8OID:
			{
				float8		dat = DatumGetFloat8(value);
				float8	   *bufptr = palloc(sizeof(float8));

				memcpy(bufptr, (char *) &dat, sizeof(float8));

				binds[attnum].buffer = bufptr;
			}
			break;
		case NUMERICOID:
			{
				Datum		valueDatum = DirectFunctionCall1(numeric_float8,
															 value);
				float8		dat = DatumGetFloat8(valueDatum);
				float8	   *bufptr = palloc(sizeof(float8));

				memcpy(bufptr, (char *) &dat, sizeof(float8));

				binds[attnum].buffer = bufptr;
			}
			break;
		case BOOLOID:
			{
				int32		dat = DatumGetInt32(value);
				int32	   *bufptr = palloc(sizeof(int32));

				memcpy(bufptr, (char *) &dat, sizeof(int32));

				binds[attnum].buffer = bufptr;
			}
			break;
		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		case ANYENUMOID:
			{
				char	   *outputString = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);

				binds[attnum].buffer = outputString;
				binds[attnum].buffer_length = strlen(outputString);
			}
			break;
		case NAMEOID:
			{
				char	   *outputString = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);

				binds[attnum].buffer = outputString;
				binds[attnum].buffer_length = strlen(outputString);
			}
			break;
		case DATEOID:
			{
				int			tz;
				struct pg_tm tt,
						   *tm = &tt;
				fsec_t		fsec;
				const char *tzn;
				Datum		valueDatum = DirectFunctionCall1(date_timestamp,
															 value);
				Timestamp	valueTimestamp = DatumGetTimestamp(valueDatum);
				MYSQL_TIME *ts = palloc0(sizeof(MYSQL_TIME));

				timestamp2tm(valueTimestamp, &tz, tm, &fsec, &tzn,
							 pg_tzset("UTC"));

				DATE_MYSQL_PG(ts, tt);

				binds[attnum].buffer = ts;
				binds[attnum].buffer_length = sizeof(MYSQL_TIME);
			}
			break;
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				Timestamp	valueTimestamp = DatumGetTimestamp(value);
				MYSQL_TIME *ts = palloc0(sizeof(MYSQL_TIME));
				int			tz;
				struct pg_tm tt,
						   *tm = &tt;
				fsec_t		fsec;
				const char *tzn;

				timestamp2tm(valueTimestamp, &tz, tm, &fsec, &tzn,
							 pg_tzset("UTC"));

				DATE_MYSQL_PG(ts, tt);

				binds[attnum].buffer = ts;
				binds[attnum].buffer_length = sizeof(MYSQL_TIME);
			}
			break;
		case BITOID:
			{
				int32		dat;
				int32	   *bufptr = palloc0(sizeof(int32));
				char	   *outputString = NULL;
				Oid			outputFunctionId = InvalidOid;
				bool		typeVarLength = false;

				getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
				outputString = OidOutputFunctionCall(outputFunctionId, value);

				dat = bin_dec(atoi(outputString));
				memcpy(bufptr, (char *) &dat, sizeof(int32));
				binds[attnum].buffer = bufptr;
			}
			break;
		case BYTEAOID:
			{
				int			len;
				char	   *dat = NULL;
				char	   *bufptr;
				char	   *result = DatumGetPointer(value);

				if (VARATT_IS_1B(result))
				{
					len = VARSIZE_1B(result) - VARHDRSZ_SHORT;
					dat = VARDATA_1B(result);
				}
				else
				{
					len = VARSIZE_4B(result) - VARHDRSZ;
					dat = VARDATA_4B(result);
				}

				bufptr = palloc(len);
				memcpy(bufptr, (char *) dat, len);
				binds[attnum].buffer = bufptr;
				binds[attnum].buffer_length = len;
			}
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("cannot convert constant value to MySQL value"),
					 errhint("Constant value data type: %u", type)));
			break;
	}
}

/*
 * mysql_bind_result:
 * 		Bind the value and null pointers to get the data from
 * 		remote mysql table (SELECT)
 */
void
mysql_bind_result(Oid pgtyp, int pgtypmod, MYSQL_FIELD *field,
				  mysql_column *column)
{
	MYSQL_BIND *mbind = column->mysql_bind;

	memset(mbind, 0, sizeof(MYSQL_BIND));

#if MYSQL_VERSION_ID < 80000 || MARIADB_VERSION_ID >= 100000
	mbind->is_null = (my_bool *) &column->is_null;
	mbind->error = (my_bool *) &column->error;
#else
	mbind->is_null = &column->is_null;
	mbind->error = &column->error;
#endif
	mbind->length = &column->length;

	switch (pgtyp)
	{
		case BYTEAOID:
			mbind->buffer_type = MYSQL_TYPE_BLOB;
			/* Leave room at front for bytea buffer length prefix */
			column->value = (Datum) palloc0(MAX_BLOB_WIDTH + VARHDRSZ);
			mbind->buffer = VARDATA(column->value);
			mbind->buffer_length = MAX_BLOB_WIDTH;
			break;
		case TEXTOID:
			mbind->buffer_type = MYSQL_TYPE_VAR_STRING;
			if (field->max_length == 0)
			{
				column->value = (Datum) palloc0(MAXDATALEN);
				mbind->buffer_length = MAXDATALEN;
			}
			else
			{
				column->value = (Datum) palloc0(field->max_length);
				mbind->buffer_length = field->max_length;
			}
			mbind->buffer = (char *) column->value;
			break;
		default:
			mbind->buffer_type = MYSQL_TYPE_VAR_STRING;
			column->value = (Datum) palloc0(MAXDATALEN);
			mbind->buffer = (char *) column->value;
			mbind->buffer_length = MAXDATALEN;
	}
}

static int
dec_bin(int number)
{
	int			rem;
	int			i = 1;
	int			bin = 0;

	while (number != 0)
	{
		rem = number % 2;
		number /= 2;
		bin += rem * i;
		i *= 10;
	}

	return bin;
}

static int
bin_dec(int binarynumber)
{
	int			dec = 0;
	int			i = 0;
	int			rem;

	while (binarynumber != 0)
	{
		rem = binarynumber % 10;
		binarynumber /= 10;
		dec += rem * pow(2, i);
		++i;
	}

	return dec;
}
