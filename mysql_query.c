/*-------------------------------------------------------------------------
 *
 * mysql_query.c
 * 		Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 *
 * Portions Copyright (c) 2004-2014, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		mysql_query.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "mysql_fdw.h"
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <mysql.h>
#include <mysql_com.h>

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
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
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
#include "catalog/pg_type.h"
#include "funcapi.h"

#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/datetime.h"


#include "mysql_fdw.h"
#include "mysql_query.h"


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
static int dec_bin(int n);
static int bin_dec(int n);


/*
 * convert_mysql_to_pg: Convert MySQL data into PostgreSQL's compatible data types
 */
Datum
mysql_convert_to_pg(Oid pgtyp, int pgtypmod, mysql_column *column)
{
	Datum value_datum = 0;
	Datum valueDatum = 0;
	regproc typeinput;
	HeapTuple tuple;
	int typemod;
	char str[MAXDATELEN];

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type%u", pgtyp);

	typeinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
	typemod  = ((Form_pg_type)GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);

	switch (pgtyp)
	{
		/*
		 * MySQL gives BIT / BIT(n) data type as decimal value. The only way to
		 * retrieve this value is to use BIN, OCT or HEX function in MySQL, otherwise
		 * mysql client shows the actual decimal value, which could be a non - printable character.
		 * For exmple in MySQL
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
		 * PostgreSQL expacts all binary data to be composed of either '0' or '1'. MySQL gives
		 * value 9 hence PostgreSQL reports error. The solution is to convert the decimal number
		 * into equivalent binary string.
		 */
		case BYTEAOID:
			SET_VARSIZE(column->value, column->length + VARHDRSZ);
			return PointerGetDatum(column->value);

		case BITOID:
			sprintf(str, "%d", dec_bin(*((int*)column->value)));
			valueDatum = CStringGetDatum((char*)str);
		break;
		default:
			valueDatum = CStringGetDatum((char*)column->value);
	}
	value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(InvalidOid), Int32GetDatum(typemod));
	return value_datum;
}


/*
 * mysql_from_pgtyp: Give MySQL data type for PG type
 */
static int32
mysql_from_pgtyp(Oid type)
{
	switch(type)
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
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("cannot convert constant value to MySQL value"),
							errhint("Constant value data type: %u", type)));
			break;
		}
	}
}

/*
 * bind_sql_var: 
 * Bind the values provided as DatumBind the values and nulls to modify the target table (INSERT/UPDATE)
 */
void 
mysql_bind_sql_var(Oid type, int attnum, Datum value, MYSQL_BIND *binds, bool *isnull)
{
	/* Clear the bind buffer and attributes */
	memset(&binds[attnum], 0x0, sizeof(MYSQL_BIND));

	binds[attnum].buffer_type = mysql_from_pgtyp(type);
	binds[attnum].is_null = isnull;

	/* Avoid to bind buffer in case value is NULL */
	if (*isnull)
		return;

	switch(type)
	{
		case INT2OID:
		{
			int16 dat = DatumGetInt16(value);
			int16 *bufptr = palloc0(sizeof(int16));
			memcpy(bufptr, (char*)&dat, sizeof(int16));

			binds[attnum].buffer = bufptr;
			break;
		}
		case INT4OID:
		{
			int32 dat = DatumGetInt32(value);
			int32 *bufptr = palloc0(sizeof(int32));
			memcpy(bufptr, (char*)&dat, sizeof(int32));

			binds[attnum].buffer = bufptr;
			break;
		}
		case INT8OID:
		{
			int64 dat = DatumGetInt64(value);
			int64 *bufptr = palloc0(sizeof(int64));
			memcpy(bufptr, (char*)&dat, sizeof(int64));

			binds[attnum].buffer = bufptr;
			break;
		}
		case FLOAT4OID:
		{
			float4 dat = DatumGetFloat4(value);
			float4 *bufptr = palloc0(sizeof(float4));
			memcpy(bufptr, (char*)&dat, sizeof(float4));

			binds[attnum].buffer = bufptr;
			break;
		}
		case FLOAT8OID:
		{
			float8 dat = DatumGetFloat8(value);
			float8 *bufptr = palloc0(sizeof(float8));
			memcpy(bufptr, (char*)&dat, sizeof(float8));

			binds[attnum].buffer = bufptr;
			break;
		}
		case NUMERICOID:
		{
			Datum valueDatum = DirectFunctionCall1(numeric_float8, value);
			float8 dat = DatumGetFloat8(valueDatum);
			float8 *bufptr = palloc0(sizeof(float8));
			memcpy(bufptr, (char*)&dat, sizeof(float8));

			binds[attnum].buffer = bufptr;
			break;
		}
		case BOOLOID:
		{
			int32 dat = DatumGetInt32(value);
			int32 *bufptr = palloc0(sizeof(int32));
			memcpy(bufptr, (char*)&dat, sizeof(int32));

			binds[attnum].buffer = bufptr;
			break;
		}

		case BPCHAROID:
		case VARCHAROID:
		case TEXTOID:
		case JSONOID:
		{
			char *outputString = NULL;
			Oid outputFunctionId = InvalidOid;
			bool typeVarLength = false;
			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, value);

			binds[attnum].buffer = outputString;
			binds[attnum].buffer_length = strlen(outputString);
			break;
		}
		case NAMEOID:
		{
			char *outputString = NULL;
			Oid outputFunctionId = InvalidOid;
			bool typeVarLength = false;
			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, value);

			binds[attnum].buffer=outputString;
			binds[attnum].buffer_length=strlen(outputString);
			break;
		}
		case DATEOID:
		{
			int tz;
			struct pg_tm tt, *tm = &tt;
			fsec_t fsec;
			const char *tzn;

			Datum valueDatum = DirectFunctionCall1(date_timestamp, value);
			Timestamp valueTimestamp = DatumGetTimestamp(valueDatum);
			MYSQL_TIME* ts = palloc0(sizeof(MYSQL_TIME));

			timestamp2tm(valueTimestamp, &tz, tm, &fsec, &tzn, pg_tzset("UTC"));

			DATE_MYSQL_PG(ts, tt);

			binds[attnum].buffer = ts;
			binds[attnum].buffer_length=sizeof(MYSQL_TIME);

			break;
		}
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			Timestamp valueTimestamp = DatumGetTimestamp(value);
			MYSQL_TIME* ts = palloc0(sizeof(MYSQL_TIME));

			int			tz;
			struct pg_tm tt,
			*tm = &tt;
			fsec_t		fsec;
			const char *tzn;

			timestamp2tm(valueTimestamp, &tz, tm, &fsec, &tzn, pg_tzset("UTC"));

			DATE_MYSQL_PG(ts, tt);

			binds[attnum].buffer = ts;
			binds[attnum].buffer_length = sizeof(MYSQL_TIME);

			break;
		}
		case BITOID:
		{
			int32 dat;
			int32 *bufptr = palloc0(sizeof(int32));
			char *outputString = NULL;
			Oid outputFunctionId = InvalidOid;
			bool typeVarLength = false;
			getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
			outputString = OidOutputFunctionCall(outputFunctionId, value);

			dat = bin_dec(atoi(outputString));
			memcpy(bufptr, (char*)&dat, sizeof(int32));
			binds[attnum].buffer = bufptr;
			break;
		}
		case BYTEAOID:
		{
			int  len;
			char *dat = NULL;
			char *bufptr;
			char *result = DatumGetPointer(value);
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
			bufptr = palloc0(len);
			memcpy(bufptr, (char*)dat, len);
			binds[attnum].buffer = bufptr;
			binds[attnum].buffer_length = len;
			break;
		}

		default:
		{
			ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("cannot convert constant value to MySQL value"),
							errhint("Constant value data type: %u", type)));
			break;
		}
	}
}


/*
 * mysql_bind_result: Bind the value and null pointers to get
 * the data from remote mysql table (SELECT)
 */
void
mysql_bind_result(Oid pgtyp, int pgtypmod, MYSQL_FIELD *field, mysql_column *column)
{
	MYSQL_BIND *mbind = column->_mysql_bind;
	mbind->is_null = &column->is_null;
	mbind->length = &column->length;
	mbind->error = &column->error;

	switch (pgtyp)
	{
			case BYTEAOID:
					mbind->buffer_type = MYSQL_TYPE_BLOB;
					/* leave room at front for bytea buffer length prefix */
					column->value = (Datum) palloc0(MAX_BLOB_WIDTH + VARHDRSZ);
					mbind->buffer = VARDATA(column->value);
					mbind->buffer_length = MAX_BLOB_WIDTH;
					break;

			default:
					mbind->buffer_type = MYSQL_TYPE_VAR_STRING;
					column->value = (Datum) palloc0(MAXDATALEN);
					mbind->buffer = (char *) column->value;
					mbind->buffer_length = MAXDATALEN;
	}
}

static
int dec_bin(int n)
{
	int rem, i = 1;
	int bin = 0;

	while (n != 0)
	{
		rem  = n % 2;
		n /= 2;
		bin += rem * i;
		i *= 10;
	}
	return bin;
}

static int
bin_dec(int n)
{
	int dec = 0;
	int i = 0;
	int rem;

	while (n != 0)
	{
		rem = n % 10;
		n /= 10;
		dec += rem * pow(2 , i);
		++i;
	}
	return dec;
}
