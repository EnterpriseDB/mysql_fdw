/*-------------------------------------------------------------------------
 *
 * deparse.c
 * 		Query deparser for mysql_fdw
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2022, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 		deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/transam.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "datatype/timestamp.h"
#include "mysql_fdw.h"
#include "mysql_pushability.h"
#include "nodes/nodeFuncs.h"
#include "nodes/plannodes.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM < 120000
#include "optimizer/var.h"
#else
#include "optimizer/optimizer.h"
#endif
#include "optimizer/prep.h"
#include "optimizer/tlist.h"
#include "parser/parsetree.h"
#include "pgtime.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"


static char *mysql_quote_identifier(const char *str, char quotechar);

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */

	/*
	 * For join pushdown, only a limited set of operators are allowed to be
	 * pushed.  This flag helps us identify if we are walking through the list
	 * of join conditions. Also true for aggregate relations to restrict
	 * aggregates for specified list.
	 */
	bool		is_remote_cond; /* true for join or aggregate relations */
	Relids		relids;			/* relids of base relations in the underlying
								 * scan */
} foreign_glob_cxt;

/*
 * Local (per-tree-level) context for foreign_expr_walker's search.
 * This is concerned with identifying collations used in the expression.
 */
typedef enum
{
	FDW_COLLATE_NONE,			/* expression is of a noncollatable type */
	FDW_COLLATE_SAFE,			/* collation derives from a foreign Var */
	FDW_COLLATE_UNSAFE			/* collation derives from something else */
} FDWCollateState;

typedef struct foreign_loc_cxt
{
	Oid			collation;		/* OID of current collation, if any */
	FDWCollateState state;		/* state of current collation choice */
} foreign_loc_cxt;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;			/* global planner state */
	RelOptInfo *foreignrel;		/* the foreign relation we are planning for */
	RelOptInfo *scanrel;		/* the underlying scan relation. Same as
								 * foreignrel, when that represents a join or
								 * a base relation. */
	StringInfo	buf;			/* output buffer to append to */
	List	  **params_list;	/* exprs that will become remote Params */
	bool		is_not_distinct_op; /* True in case of IS NOT DISTINCT clause */
} deparse_expr_cxt;

#define REL_ALIAS_PREFIX	"r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)	\
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))

/*
 * Functions to construct string representation of a node tree.
 */
static void deparseExpr(Expr *expr, deparse_expr_cxt *context);
static void mysql_deparse_var(Var *node, deparse_expr_cxt *context);
static void mysql_deparse_const(Const *node, deparse_expr_cxt *context);
static void mysql_deparse_param(Param *node, deparse_expr_cxt *context);
#if PG_VERSION_NUM < 120000
static void mysql_deparse_array_ref(ArrayRef *node, deparse_expr_cxt *context);
#else
static void mysql_deparse_array_ref(SubscriptingRef *node,
									deparse_expr_cxt *context);
#endif
static void mysql_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void mysql_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void mysql_deparse_operator_name(StringInfo buf,
										Form_pg_operator opform);
static void mysql_deparse_distinct_expr(DistinctExpr *node,
										deparse_expr_cxt *context);
static void mysql_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
											   deparse_expr_cxt *context);
static void mysql_deparse_relabel_type(RelabelType *node,
									   deparse_expr_cxt *context);
static void mysql_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void mysql_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void mysql_deparse_array_expr(ArrayExpr *node,
									 deparse_expr_cxt *context);
static void mysql_print_remote_param(int paramindex, Oid paramtype,
									 int32 paramtypmod,
									 deparse_expr_cxt *context);
static void mysql_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
										   deparse_expr_cxt *context);
static void mysql_deparse_relation(StringInfo buf, Relation rel);
static void mysql_deparse_target_list(StringInfo buf, PlannerInfo *root,
									  Index rtindex, Relation rel,
									  Bitmapset *attrs_used,
									  List **retrieved_attrs);
static void mysql_deparse_column_ref(StringInfo buf, int varno, int varattno,
									 PlannerInfo *root, bool qualify_col);
static void mysql_deparse_select_sql(List *tlist, List **retrieved_attrs,
									 deparse_expr_cxt *context);
static void mysql_append_conditions(List *exprs, deparse_expr_cxt *context);
static void mysql_deparse_explicit_target_list(List *tlist,
											   List **retrieved_attrs,
											   deparse_expr_cxt *context);
static void mysql_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
											RelOptInfo *foreignrel,
											bool use_alias, List **param_list);
static void mysql_deparse_from_expr(List *quals, deparse_expr_cxt *context);
static void mysql_append_function_name(Oid funcid, deparse_expr_cxt *context);
static void mysql_deparse_aggref(Aggref *node, deparse_expr_cxt *context);
static void mysql_append_groupby_clause(List *tlist, deparse_expr_cxt *context);
static Node *mysql_deparse_sort_group_clause(Index ref, List *tlist,
											 bool force_colno,
											 deparse_expr_cxt *context);
static void mysql_append_orderby_clause(List *pathkeys, bool has_final_sort,
										deparse_expr_cxt *context);
static void mysql_append_limit_clause(deparse_expr_cxt *context);
static void mysql_append_orderby_suffix(Expr *em_expr, const char *sortby_dir,
										Oid sortcoltype, bool nulls_first,
										deparse_expr_cxt *context);

/*
 * Functions to construct string representation of a specific types.
 */
static void deparse_interval(StringInfo buf, Datum datum);

/*
 * Local variables.
 */
static char *cur_opname = NULL;

/*
 * Append remote name of specified foreign table to buf.  Use value of
 * table_name FDW option (if any) instead of relation's name.  Similarly,
 * schema_name FDW option overrides schema name.
 */
static void
mysql_deparse_relation(StringInfo buf, Relation rel)
{
	ForeignTable *table;
	const char *nspname = NULL;
	const char *relname = NULL;
	ListCell   *lc;

	/* Obtain additional catalog information. */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * Use value of FDW options if any, instead of the name of object itself.
	 */
	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "dbname") == 0)
			nspname = defGetString(def);
		else if (strcmp(def->defname, "table_name") == 0)
			relname = defGetString(def);
	}

	/*
	 * Note: we could skip printing the schema name if it's pg_catalog, but
	 * that doesn't seem worth the trouble.
	 */
	if (nspname == NULL)
		nspname = get_namespace_name(RelationGetNamespace(rel));
	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	appendStringInfo(buf, "%s.%s", mysql_quote_identifier(nspname, '`'),
					 mysql_quote_identifier(relname, '`'));
}

static char *
mysql_quote_identifier(const char *str, char quotechar)
{
	char	   *result = palloc(strlen(str) * 2 + 3);
	char	   *res = result;

	*res++ = quotechar;
	while (*str)
	{
		if (*str == quotechar)
			*res++ = *str;
		*res++ = *str;
		str++;
	}
	*res++ = quotechar;
	*res++ = '\0';

	return result;
}

/*
 * mysql_deparse_select_stmt_for_rel
 * 		Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from foreign
 * server.  For a base relation fpinfo->attrs_used is used to construct
 * SELECT clause, hence the tlist is ignored for a base relation.
 *
 * remote_conds is the list of conditions to be deparsed into the WHERE clause.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the remote
 * server as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
extern void
mysql_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root,
								  RelOptInfo *rel, List *tlist,
								  List *remote_conds, List *pathkeys,
								  bool has_final_sort, bool has_limit,
								  List **retrieved_attrs, List **params_list)
{
	deparse_expr_cxt context;
	List	   *quals;
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) rel->fdw_private;

	/*
	 * We handle relations for foreign tables and joins between those and
	 * upper relations.
	 */
	Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel));

	/* Fill portions of context common to base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.params_list = params_list;
	context.scanrel = IS_UPPER_REL(rel) ? fpinfo->outerrel : rel;
	context.is_not_distinct_op = false;

	/* Construct SELECT clause */
	mysql_deparse_select_sql(tlist, retrieved_attrs, &context);

	/*
	 * For upper relations, the WHERE clause is built from the remote
	 * conditions of the underlying scan relation; otherwise, we can use the
	 * supplied list of remote conditions directly.
	 */
	if (IS_UPPER_REL(rel))
	{
		MySQLFdwRelationInfo *ofpinfo;

		ofpinfo = (MySQLFdwRelationInfo *) fpinfo->outerrel->fdw_private;
		quals = ofpinfo->remote_conds;
	}
	else
		quals = remote_conds;

	/* Construct FROM and WHERE clauses */
	mysql_deparse_from_expr(quals, &context);

	if (IS_UPPER_REL(rel))
	{
		/* Append GROUP BY clause */
		mysql_append_groupby_clause(fpinfo->grouped_tlist, &context);

		/* Append HAVING clause */
		if (remote_conds)
		{
			appendStringInfoString(buf, " HAVING ");
			mysql_append_conditions(remote_conds, &context);
		}
	}

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		mysql_append_orderby_clause(pathkeys, has_final_sort, &context);

	/* Add LIMIT clause if necessary */
	if (has_limit)
		mysql_append_limit_clause(&context);
}

/*
 * mysql_deparse_select_sql
 * 		Construct a simple SELECT statement that retrieves desired columns
 * 		of the specified foreign table, and append it to "buf".  The output
 * 		contains just "SELECT ...".
 *
 * tlist is the list of desired columns.  Read prologue of
 * mysql_deparse_select_stmt_for_rel() for details.
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs.
 */
static void
mysql_deparse_select_sql(List *tlist, List **retrieved_attrs,
						 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	PlannerInfo *root = context->root;

	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");

	if (IS_JOIN_REL(foreignrel) || IS_UPPER_REL(foreignrel))
	{
		/*
		 * For a join or upper relation the input tlist gives the list of
		 * columns required to be fetched from the foreign server.
		 */
		mysql_deparse_explicit_target_list(tlist, retrieved_attrs, context);
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);
		Relation	rel;
		MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) foreignrel->fdw_private;

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
#if PG_VERSION_NUM < 130000
		rel = heap_open(rte->relid, NoLock);
#else
		rel = table_open(rte->relid, NoLock);
#endif

		mysql_deparse_target_list(buf, root, foreignrel->relid, rel,
								  fpinfo->attrs_used, retrieved_attrs);

#if PG_VERSION_NUM < 130000
		heap_close(rel, NoLock);
#else
		table_close(rel, NoLock);
#endif
	}
}

/*
 * mysql_deparse_explicit_target_list
 * 		Deparse given targetlist and append it to context->buf.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1.  It has same number of entries as tlist.
 */
static void
mysql_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,
								   deparse_expr_cxt *context)
{
	ListCell   *lc;
	StringInfo	buf = context->buf;
	int			i = 0;

	*retrieved_attrs = NIL;

	foreach(lc, tlist)
	{
		if (i > 0)
			appendStringInfoString(buf, ", ");

		deparseExpr((Expr *) lfirst(lc), context);
		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0)
		appendStringInfoString(buf, "NULL");
}

/*
 * mysql_deparse_from_expr
 * 		Construct a FROM clause and, if needed, a WHERE clause, and
 * 		append those to "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 */
static void
mysql_deparse_from_expr(List *quals, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;

	/* For upper relations, scanrel must be either a joinrel or a baserel */
	Assert(!IS_UPPER_REL(context->foreignrel) ||
		   IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	mysql_deparse_from_expr_for_rel(buf, context->root, scanrel,
									(bms_membership(scanrel->relids) == BMS_MULTIPLE),
									context->params_list);

	/* Construct WHERE clause */
	if (quals != NIL)
	{
		appendStringInfoString(buf, " WHERE ");
		mysql_append_conditions(quals, context);
	}
}

/*
 * Deparse remote INSERT statement
 */
void
mysql_deparse_insert(StringInfo buf, PlannerInfo *root, Index rtindex,
					 Relation rel, List *targetAttrs, bool doNothing)
{
	ListCell   *lc;
#if PG_VERSION_NUM >= 140000
	TupleDesc	tupdesc = RelationGetDescr(rel);
#endif

	appendStringInfo(buf, "INSERT %sINTO ", doNothing ? "IGNORE " : "");
	mysql_deparse_relation(buf, rel);

	if (targetAttrs)
	{
		AttrNumber	pindex;
		bool		first;

		appendStringInfoChar(buf, '(');

		first = true;
		foreach(lc, targetAttrs)
		{
			int			attnum = lfirst_int(lc);

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			mysql_deparse_column_ref(buf, rtindex, attnum, root, false);
		}

		appendStringInfoString(buf, ") VALUES (");

		pindex = 1;
		first = true;
		foreach(lc, targetAttrs)
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

#if PG_VERSION_NUM >= 140000
			if (TupleDescAttr(tupdesc, lfirst_int(lc) - 1)->attgenerated)
			{
				appendStringInfoString(buf, "DEFAULT");
				continue;
			}
#endif
			appendStringInfo(buf, "?");
			pindex++;
		}

		appendStringInfoChar(buf, ')');
	}
	else
		appendStringInfoString(buf, " DEFAULT VALUES");
}

void
mysql_deparse_analyze(StringInfo sql, char *dbname, char *relname)
{
	appendStringInfo(sql, "SELECT");
	appendStringInfo(sql, " round(((data_length + index_length)), 2)");
	appendStringInfo(sql, " FROM information_schema.TABLES");
	appendStringInfo(sql, " WHERE table_schema = '%s' AND table_name = '%s'",
					 dbname, relname);
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists.
 */
static void
mysql_deparse_target_list(StringInfo buf, PlannerInfo *root, Index rtindex,
						  Relation rel, Bitmapset *attrs_used,
						  List **retrieved_attrs)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	bool		have_wholerow;
	bool		first;
	int			i;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	first = true;

	*retrieved_attrs = NIL;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			mysql_deparse_column_ref(buf, rtindex, i, root, false);
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* Don't generate bad syntax if no undropped columns */
	if (first)
		appendStringInfoString(buf, "NULL");
}

/*
 * Construct name to use for given column, and emit it into buf.  If it has a
 * column_name FDW option, use that instead of attribute name.
 */
static void
mysql_deparse_column_ref(StringInfo buf, int varno, int varattno,
						 PlannerInfo *root, bool qualify_col)
{
	RangeTblEntry *rte;
	char	   *colname = NULL;
	List	   *options;
	ListCell   *lc;

	/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* Get RangeTblEntry from array in PlannerInfo. */
	rte = planner_rt_fetch(varno, root);

	/*
	 * If it's a column of a foreign table, and it has the column_name FDW
	 * option, use that value.
	 */
	options = GetForeignColumnOptions(rte->relid, varattno);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/*
	 * If it's a column of a regular table or it doesn't have column_name FDW
	 * option, use attribute name.
	 */
	if (colname == NULL)
#if PG_VERSION_NUM >= 110000
		colname = get_attname(rte->relid, varattno, false);
#else
		colname = get_relid_attribute_name(rte->relid, varattno);
#endif

	if (qualify_col)
		ADD_REL_QUALIFIER(buf, varno);

	appendStringInfoString(buf, mysql_quote_identifier(colname, '`'));
}

static void
mysql_deparse_string(StringInfo buf, const char *val, bool isstr)
{
	const char *valptr;
	int			i = 0;

	if (isstr)
		appendStringInfoChar(buf, '\'');

	for (valptr = val; *valptr; valptr++, i++)
	{
		char		ch = *valptr;

		/*
		 * Remove '{', '}', and \" character from the string. Because this
		 * syntax is not recognize by the remote MySQL server.
		 */
		if ((ch == '{' && i == 0) || (ch == '}' && (i == (strlen(val) - 1))) ||
			ch == '\"')
			continue;

		if (isstr && ch == ',')
		{
			appendStringInfoString(buf, "', '");
			continue;
		}
		appendStringInfoChar(buf, ch);
	}

	if (isstr)
		appendStringInfoChar(buf, '\'');
}

/*
* Append a SQL string literal representing "val" to buf.
*/
static void
mysql_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	appendStringInfoChar(buf, '\'');

	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}

	appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
deparseExpr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			mysql_deparse_var((Var *) node, context);
			break;
		case T_Const:
			mysql_deparse_const((Const *) node, context);
			break;
		case T_Param:
			mysql_deparse_param((Param *) node, context);
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
			mysql_deparse_array_ref((ArrayRef *) node, context);
#else
		case T_SubscriptingRef:
			mysql_deparse_array_ref((SubscriptingRef *) node, context);
#endif
			break;
		case T_FuncExpr:
			mysql_deparse_func_expr((FuncExpr *) node, context);
			break;
		case T_OpExpr:
			mysql_deparse_op_expr((OpExpr *) node, context);
			break;
		case T_DistinctExpr:
			mysql_deparse_distinct_expr((DistinctExpr *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			mysql_deparse_scalar_array_op_expr((ScalarArrayOpExpr *) node,
											   context);
			break;
		case T_RelabelType:
			mysql_deparse_relabel_type((RelabelType *) node, context);
			break;
		case T_BoolExpr:
			mysql_deparse_bool_expr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			mysql_deparse_null_test((NullTest *) node, context);
			break;
		case T_ArrayExpr:
			mysql_deparse_array_expr((ArrayExpr *) node, context);
			break;
		case T_Aggref:
			mysql_deparse_aggref((Aggref *) node, context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d",
				 (int) nodeTag(node));
			break;
	}
}

/*
 * Deparse Interval type into MySQL Interval representation.
 */
static void
deparse_interval(StringInfo buf, Datum datum)
{
	struct pg_tm tm;
	fsec_t		fsec;
	bool		is_first = true;
#if PG_VERSION_NUM >= 150000
	struct pg_itm tt,
			   *itm = &tt;
#endif

#define append_interval(expr, unit) \
do { \
	if (!is_first) \
		appendStringInfo(buf, " %s ", cur_opname); \
	appendStringInfo(buf, "INTERVAL %d %s", expr, unit); \
	is_first = false; \
} while (0)

	/* Check saved opname. It could be only "+" and "-" */
	Assert(cur_opname);

#if PG_VERSION_NUM >= 150000
	interval2itm(*DatumGetIntervalP(datum), itm);
	tm.tm_sec = itm->tm_sec;
	tm.tm_min = itm->tm_min;
	tm.tm_hour = itm->tm_hour;
	tm.tm_mday = itm->tm_mday;
	tm.tm_mon = itm->tm_mon;
	tm.tm_year = itm->tm_year;
	fsec = itm->tm_usec;
#else
	if (interval2tm(*DatumGetIntervalP(datum), &tm, &fsec) != 0)
		elog(ERROR, "could not convert interval to tm");
#endif

	if (tm.tm_year > 0)
		append_interval(tm.tm_year, "YEAR");

	if (tm.tm_mon > 0)
		append_interval(tm.tm_mon, "MONTH");

	if (tm.tm_mday > 0)
		append_interval(tm.tm_mday, "DAY");

	if (tm.tm_hour > 0)
		append_interval(tm.tm_hour, "HOUR");

	if (tm.tm_min > 0)
		append_interval(tm.tm_min, "MINUTE");

	if (tm.tm_sec > 0)
		append_interval(tm.tm_sec, "SECOND");

	if (fsec > 0)
	{
		if (!is_first)
			appendStringInfo(buf, " %s ", cur_opname);
#ifdef HAVE_INT64_TIMESTAMP
		appendStringInfo(buf, "INTERVAL %d MICROSECOND", fsec);
#else
		appendStringInfo(buf, "INTERVAL %f MICROSECOND", fsec);
#endif
	}
}

/*
 * Deparse remote UPDATE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
mysql_deparse_update(StringInfo buf, PlannerInfo *root, Index rtindex,
					 Relation rel, List *targetAttrs, char *attname)
{
	AttrNumber	pindex;
	bool		first;
	ListCell   *lc;
#if PG_VERSION_NUM >= 140000
	TupleDesc	tupdesc = RelationGetDescr(rel);
#endif

	appendStringInfoString(buf, "UPDATE ");
	mysql_deparse_relation(buf, rel);
	appendStringInfoString(buf, " SET ");

	pindex = 2;
	first = true;
	foreach(lc, targetAttrs)
	{
		int			attnum = lfirst_int(lc);

		if (attnum == 1)
			continue;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		mysql_deparse_column_ref(buf, rtindex, attnum, root, false);

#if PG_VERSION_NUM >= 140000
		if (TupleDescAttr(tupdesc, attnum - 1)->attgenerated)
		{
			appendStringInfoString(buf, " = DEFAULT");
			continue;
		}
#endif
		appendStringInfo(buf, " = ?");
		pindex++;
	}

	appendStringInfo(buf, " WHERE %s = ?", attname);
}

/*
 * Deparse remote DELETE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
mysql_deparse_delete(StringInfo buf, PlannerInfo *root, Index rtindex,
					 Relation rel, char *name)
{
	appendStringInfoString(buf, "DELETE FROM ");
	mysql_deparse_relation(buf, rel);
	appendStringInfo(buf, " WHERE %s = ?", name);
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
mysql_deparse_var(Var *node, deparse_expr_cxt *context)
{
	Relids		relids = context->scanrel->relids;
	bool		qualify_col;

	qualify_col = (bms_membership(relids) == BMS_MULTIPLE);

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
	{
		/* Var belongs to foreign table */
		mysql_deparse_column_ref(context->buf, node->varno, node->varattno,
								 context->root, qualify_col);
	}
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int			pindex = 0;
			ListCell   *lc;

			/* Find its index in params_list */
			foreach(lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *) lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				/* Not in list, so add it */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}
			mysql_print_remote_param(pindex, node->vartype, node->vartypmod,
									 context);
		}
		else
			mysql_print_remote_placeholder(node->vartype, node->vartypmod,
										   context);
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 */
static void
mysql_deparse_const(Const *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);

	switch (node->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			{
				extval = OidOutputFunctionCall(typoutput, node->constvalue);

				/*
				 * No need to quote unless it's a special value such as 'NaN'.
				 * See comments in get_const_expr().
				 */
				if (strspn(extval, "0123456789+-eE.") == strlen(extval))
				{
					if (extval[0] == '+' || extval[0] == '-')
						appendStringInfo(buf, "(%s)", extval);
					else
						appendStringInfoString(buf, extval);
				}
				else
					appendStringInfo(buf, "'%s'", extval);
			}
			break;
		case BITOID:
		case VARBITOID:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			appendStringInfo(buf, "B'%s'", extval);
			break;
		case BOOLOID:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;
		case INTERVALOID:
			deparse_interval(buf, node->constvalue);
			break;
		case BYTEAOID:

			/*
			 * The string for BYTEA always seems to be in the format "\\x##"
			 * where # is a hex digit, Even if the value passed in is
			 * 'hi'::bytea we will receive "\x6869". Making this assumption
			 * allows us to quickly convert postgres escaped strings to mysql
			 * ones for comparison
			 */
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			appendStringInfo(buf, "X\'%s\'", extval + 2);
			break;
		default:
			extval = OidOutputFunctionCall(typoutput, node->constvalue);
			mysql_deparse_string_literal(buf, extval);
			break;
	}
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list if it's not already present, and then use its index
 * in that list as the remote parameter number.  During EXPLAIN, there's
 * no need to identify a parameter number.
 */
static void
mysql_deparse_param(Param *node, deparse_expr_cxt *context)
{
	if (context->params_list)
	{
		int			pindex = 0;
		ListCell   *lc;

		/* Find its index in params_list */
		foreach(lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *) lfirst(lc)))
				break;
		}
		if (lc == NULL)
		{
			/* Not in list, so add it */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		mysql_print_remote_param(pindex, node->paramtype, node->paramtypmod,
								 context);
	}
	else
		mysql_print_remote_placeholder(node->paramtype, node->paramtypmod,
									   context);
}

/*
 * Deparse an array subscript expression.
 */
static void
#if PG_VERSION_NUM < 120000
mysql_deparse_array_ref(ArrayRef *node, deparse_expr_cxt *context)
#else
mysql_deparse_array_ref(SubscriptingRef *node, deparse_expr_cxt *context)
#endif
{
	StringInfo	buf = context->buf;
	ListCell   *lowlist_item;
	ListCell   *uplist_item;

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/*
	 * Deparse referenced array expression first.  If that expression includes
	 * a cast, we have to parenthesize to prevent the array subscript from
	 * being taken as typename decoration.  We can avoid that in the typical
	 * case of subscripting a Var, but otherwise do it.
	 */
	if (IsA(node->refexpr, Var))
		deparseExpr(node->refexpr, context);
	else
	{
		appendStringInfoChar(buf, '(');
		deparseExpr(node->refexpr, context);
		appendStringInfoChar(buf, ')');
	}

	/* Deparse subscript expressions. */
	lowlist_item = list_head(node->reflowerindexpr);	/* could be NULL */
	foreach(uplist_item, node->refupperindexpr)
	{
		appendStringInfoChar(buf, '[');
		if (lowlist_item)
		{
			deparseExpr(lfirst(lowlist_item), context);
			appendStringInfoChar(buf, ':');
#if PG_VERSION_NUM < 130000
			lowlist_item = lnext(lowlist_item);
#else
			lowlist_item = lnext(node->reflowerindexpr, lowlist_item);
#endif
		}
		deparseExpr(lfirst(uplist_item), context);
		appendStringInfoChar(buf, ']');
	}

	appendStringInfoChar(buf, ')');
}

/*
 * This is possible that the name of function in PostgreSQL and mysql differ,
 * so return the mysql eloquent function name.
 */
static char *
mysql_replace_function(char *in)
{
	if (strcmp(in, "btrim") == 0)
		return "trim";

	return in;
}

/*
 * Deparse a function call.
 */
static void
mysql_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first;
	ListCell   *arg;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/* If the function call came from a cast, then show the first argument. */
	if (node->funcformat == COERCE_EXPLICIT_CAST)
	{
		int32		coercedTypmod;

		/* Get the typmod if this is a length-coercion function */
		(void) exprIsLengthCoercion((Node *) node, &coercedTypmod);

		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * Normal function: display as proname(args).
	 */
	mysql_append_function_name(node->funcid, context);
	appendStringInfoChar(buf, '(');

	/* ... and all the arguments */
	first = true;
	foreach(arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		deparseExpr((Expr *) lfirst(arg), context);
		first = false;
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given operator expression.  To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
mysql_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Form_pg_operator form;
	char		oprkind;
	ListCell   *arg;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);

	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
		   (oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		deparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	mysql_deparse_operator_name(buf, form);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		deparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
mysql_deparse_operator_name(StringInfo buf, Form_pg_operator opform)
{
	/* opname is not a SQL identifier, so we should not quote it. */
	cur_opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		/* Print fully qualified operator name. */
		appendStringInfo(buf, "OPERATOR(%s.%s)",
						 mysql_quote_identifier(opnspname, '`'), cur_opname);
	}
	else
	{
		if (strcmp(cur_opname, "~~") == 0)
			appendStringInfoString(buf, "LIKE BINARY");
		else if (strcmp(cur_opname, "~~*") == 0)
			appendStringInfoString(buf, "LIKE");
		else if (strcmp(cur_opname, "!~~") == 0)
			appendStringInfoString(buf, "NOT LIKE BINARY");
		else if (strcmp(cur_opname, "!~~*") == 0)
			appendStringInfoString(buf, "NOT LIKE");
		else if (strcmp(cur_opname, "~") == 0)
			appendStringInfoString(buf, "REGEXP BINARY");
		else if (strcmp(cur_opname, "~*") == 0)
			appendStringInfoString(buf, "REGEXP");
		else if (strcmp(cur_opname, "!~") == 0)
			appendStringInfoString(buf, "NOT REGEXP BINARY");
		else if (strcmp(cur_opname, "!~*") == 0)
			appendStringInfoString(buf, "NOT REGEXP");
		else
			appendStringInfoString(buf, cur_opname);
	}
}

/*
 * Deparse IS [NOT] DISTINCT FROM into MySQL equivalent form.
 */
static void
mysql_deparse_distinct_expr(DistinctExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		is_not_distinct_op = context->is_not_distinct_op;

	Assert(list_length(node->args) == 2);

	/*
	 * In MySQL, <=> operator is equal to IS NOT DISTINCT FROM clause, so for
	 * IS DISTINCT FROM clause, we add NOT before <=> operator.
	 */
	if (!is_not_distinct_op)
		appendStringInfoString(buf, "(NOT ");

	/* Reset the value of is_not_distinct_op for recursive calls. */
	context->is_not_distinct_op = false;
	appendStringInfoChar(buf, '(');
	deparseExpr(linitial(node->args), context);
	appendStringInfoString(buf, " <=> ");
	deparseExpr(lsecond(node->args), context);
	appendStringInfoChar(buf, ')');

	if (!is_not_distinct_op)
		appendStringInfoString(buf, ")");
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
mysql_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
								   deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	tuple;
	Expr	   *arg1;
	Expr	   *arg2;
	Form_pg_operator form;
	char	   *opname;
	Oid			typoutput;
	bool		typIsVarlena;
	char	   *extval;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Deparse left operand. */
	arg1 = linitial(node->args);
	deparseExpr(arg1, context);
	appendStringInfoChar(buf, ' ');

	opname = NameStr(form->oprname);
	if (strcmp(opname, "<>") == 0)
		appendStringInfo(buf, " NOT ");

	/* Deparse operator name plus decoration. */
	appendStringInfo(buf, " IN (");

	/* Deparse right operand. */
	arg2 = lsecond(node->args);
	switch (nodeTag((Node *) arg2))
	{
		case T_Const:
			{
				Const	   *c = (Const *) arg2;

				if (c->constisnull)
				{
					appendStringInfoString(buf, " NULL");
					ReleaseSysCache(tuple);
					return;
				}

				getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);
				extval = OidOutputFunctionCall(typoutput, c->constvalue);

				switch (c->consttype)
				{
					case INT4ARRAYOID:
					case OIDARRAYOID:
						mysql_deparse_string(buf, extval, false);
						break;
					default:
						mysql_deparse_string(buf, extval, true);
						break;
				}
			}
			break;
		default:
			deparseExpr(arg2, context);
			break;
	}
	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
mysql_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	deparseExpr(node->arg, context);
}

/*
 * Deparse a BoolExpr node.
 *
 * Note: by the time we get here, AND and OR expressions have been flattened
 * into N-argument form, so we'd better be prepared to deal with that.
 */
static void
mysql_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	const char *op = NULL;		/* keep compiler quiet */
	bool		first;
	ListCell   *lc;
	Expr	   *arg;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			/*
			 * Per transformAExprDistinct(), in the case of IS NOT DISTINCT
			 * clause, it adds NOT on top of DistinctExpr.  However, in MySQL,
			 * <=> is equivalent to IS NOT DISTINCT FROM clause, so do not
			 * append NOT here if it is a DistinctExpr.
			 */
			arg = (Expr *) lfirst(list_head(node->args));
			if (!IsA(arg, DistinctExpr))
			{
				appendStringInfoString(buf, "(NOT ");
				deparseExpr(arg, context);
				appendStringInfoChar(buf, ')');
				return;
			}

			/* Mark that we are deparsing a IS NOT DISTINCT FROM clause. */
			context->is_not_distinct_op = true;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach(lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		deparseExpr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
mysql_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfoChar(buf, '(');
	deparseExpr(node->arg, context);
	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " IS NULL");
	else
		appendStringInfoString(buf, " IS NOT NULL");
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
mysql_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	bool		first = true;
	ListCell   *lc;

	appendStringInfoString(buf, "ARRAY[");
	foreach(lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		deparseExpr(lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ']');
}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void
mysql_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,
						 deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfo(buf, "?");
}

static void
mysql_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
							   deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	appendStringInfo(buf, "(SELECT null)");
}

/*
 * Return true if given object is one of PostgreSQL's built-in objects.
 *
 * We use FirstBootstrapObjectId as the cutoff, so that we only consider
 * objects with hand-assigned OIDs to be "built in", not for instance any
 * function or type defined in the information_schema.
 *
 * Our constraints for dealing with types are tighter than they are for
 * functions or operators: we want to accept only types that are in pg_catalog,
 * else format_type might incorrectly fail to schema-qualify their names.
 * (This could be fixed with some changes to format_type, but for now there's
 * no need.)  Thus we must exclude information_schema types.
 *
 * XXX there is a problem with this, which is that the set of built-in
 * objects expands over time.  Something that is built-in to us might not
 * be known to the remote server, if it's of an older version.  But keeping
 * track of that would be a huge exercise.
 */
bool
mysql_is_builtin(Oid oid)
{
#if PG_VERSION_NUM >= 150000
	return (oid < FirstGenbkiObjectId);
#else
	return (oid < FirstBootstrapObjectId);
#endif
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * In addition, *outer_cxt is updated with collation information.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (which we approximate
 * as being built-in), and that all collations used in the expression derive
 * from Vars of the foreign table.  Because of the latter, the logic is pretty
 * close to assign_collations_walker() in parse_collate.c, though we can assume
 * here that the given expression is valid.
 */
static bool
foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt,
					foreign_loc_cxt *outer_cxt)
{
	bool		check_type = true;
	foreign_loc_cxt inner_cxt;
	Oid			collation;
	FDWCollateState state;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	/* Set up inner_cxt for possible recursion to child nodes */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/*
				 * If the Var is from the foreign table, we consider its
				 * collation (if any) safe to use.  If it is from another
				 * table, we treat its collation the same way as we would a
				 * Param's collation, i.e. it's not safe for it to have a
				 * non-default collation.
				 */
				if (bms_is_member(var->varno, glob_cxt->relids) &&
					var->varlevelsup == 0)
				{
					/* Var belongs to foreign table */
					collation = var->varcollid;
					state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
				}
				else
				{
					/* Var belongs to some other table */
					if (var->varcollid != InvalidOid &&
						var->varcollid != DEFAULT_COLLATION_OID)
						return false;

					/* We can consider that it doesn't set collation */
					collation = InvalidOid;
					state = FDW_COLLATE_NONE;
				}
			}
			break;
		case T_Const:
			{
				Const	   *c = (Const *) node;

				/*
				 * If the constant has non default collation, either it's of a
				 * non-built in type, or it reflects folding of a CollateExpr;
				 * either way, it's unsafe to send to the remote.
				 */
				if (c->constcollid != InvalidOid &&
					c->constcollid != DEFAULT_COLLATION_OID)
					return false;

				/* Otherwise, we can consider that it doesn't set collation */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_Param:
			{
				Param	   *p = (Param *) node;

				/*
				 * Collation rule is same as for Consts and non-foreign Vars.
				 */
				collation = p->paramcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
			{
				ArrayRef   *ar = (ArrayRef *) node;
#else
		case T_SubscriptingRef:
			{
				SubscriptingRef *ar = (SubscriptingRef *) node;
#endif

				/* Should not be in the join clauses of the Join-pushdown */
				if (glob_cxt->is_remote_cond)
					return false;

				/* Assignment should not be in restrictions. */
				if (ar->refassgnexpr != NULL)
					return false;

				/*
				 * Recurse to remaining subexpressions.  Since the array
				 * subscripts must yield (noncollatable) integers, they won't
				 * affect the inner_cxt state.
				 */
				if (!foreign_expr_walker((Node *) ar->refupperindexpr,
										 glob_cxt, &inner_cxt))
					return false;
				if (!foreign_expr_walker((Node *) ar->reflowerindexpr,
										 glob_cxt, &inner_cxt))
					return false;
				if (!foreign_expr_walker((Node *) ar->refexpr,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * Array subscripting should yield same collation as input,
				 * but for safety use same logic as for function nodes.
				 */
				collation = ar->refcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_FuncExpr:
			{
				FuncExpr   *fe = (FuncExpr *) node;

				if (!mysql_check_remote_pushability(fe->funcid))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) fe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If function's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (fe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 fe->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = fe->funccollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_OpExpr:
		case T_DistinctExpr:	/* struct-equivalent to OpExpr */
			{
				OpExpr	   *oe = (OpExpr *) node;

				if (!mysql_check_remote_pushability(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Result-collation handling is same as for functions */
				collation = oe->opcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *) node;

				if (!mysql_check_remote_pushability(oe->opno))
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) oe->args,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * If operator's input collation is not derived from a foreign
				 * Var, it can't be sent to remote.
				 */
				if (oe->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 oe->inputcollid != inner_cxt.collation)
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_RelabelType:
			{
				RelabelType *r = (RelabelType *) node;

				/*
				 * Recurse to input subexpression.
				 */
				if (!foreign_expr_walker((Node *) r->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * RelabelType must not introduce a collation not derived from
				 * an input foreign Var.
				 */
				collation = r->resultcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_BoolExpr:
			{
				BoolExpr   *b = (BoolExpr *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) b->args,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_NullTest:
			{
				NullTest   *nt = (NullTest *) node;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) nt->arg,
										 glob_cxt, &inner_cxt))
					return false;

				/* Output is always boolean and so noncollatable. */
				collation = InvalidOid;
				state = FDW_COLLATE_NONE;
			}
			break;
		case T_ArrayExpr:
			{
				ArrayExpr  *a = (ArrayExpr *) node;

				/* Should not be in the join clauses of the Join-pushdown */
				if (glob_cxt->is_remote_cond)
					return false;

				/*
				 * Recurse to input subexpressions.
				 */
				if (!foreign_expr_walker((Node *) a->elements,
										 glob_cxt, &inner_cxt))
					return false;

				/*
				 * ArrayExpr must not introduce a collation not derived from
				 * an input foreign Var.
				 */
				collation = a->array_collid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		case T_List:
			{
				List	   *l = (List *) node;
				ListCell   *lc;

				/*
				 * Recurse to component subexpressions.
				 */
				foreach(lc, l)
				{
					if (!foreign_expr_walker((Node *) lfirst(lc),
											 glob_cxt, &inner_cxt))
						return false;
				}

				/*
				 * When processing a list, collation state just bubbles up
				 * from the list elements.
				 */
				collation = inner_cxt.collation;
				state = inner_cxt.state;

				/* Don't apply exprType() to the list. */
				check_type = false;
			}
			break;
		case T_Aggref:
			{
				Aggref	   *agg = (Aggref *) node;
				ListCell   *lc;

				/* Not safe to pushdown when not in grouping context */
				if (!IS_UPPER_REL(glob_cxt->foreignrel))
					return false;

				/* Only non-split aggregates are pushable. */
				if (agg->aggsplit != AGGSPLIT_SIMPLE)
					return false;

				if (!mysql_check_remote_pushability(agg->aggfnoid))
					return false;

				/*
				 * Recurse to input args. aggdirectargs, aggorder and
				 * aggdistinct are all present in args, so no need to check
				 * their shippability explicitly.
				 */
				foreach(lc, agg->args)
				{
					Node	   *n = (Node *) lfirst(lc);

					/* If TargetEntry, extract the expression from it */
					if (IsA(n, TargetEntry))
					{
						TargetEntry *tle = (TargetEntry *) n;

						n = (Node *) tle->expr;
					}

					if (!foreign_expr_walker(n, glob_cxt, &inner_cxt))
						return false;
				}

				/* Aggregates with order are not supported on MySQL. */
				if (agg->aggorder)
					return false;

				/* FILTER clause is not supported on MySQL. */
				if (agg->aggfilter)
					return false;

				/* VARIADIC not supported on MySQL. */
				if (agg->aggvariadic)
					return false;

				/*
				 * If aggregate's input collation is not derived from a
				 * foreign Var, it can't be sent to remote.
				 */
				if (agg->inputcollid == InvalidOid)
					 /* OK, inputs are all noncollatable */ ;
				else if (inner_cxt.state != FDW_COLLATE_SAFE ||
						 agg->inputcollid != inner_cxt.collation)
					return false;

				/*
				 * Detect whether node is introducing a collation not derived
				 * from a foreign Var.  (If so, we just mark it unsafe for now
				 * rather than immediately returning false, since the parent
				 * node might not care.)
				 */
				collation = agg->aggcollid;
				if (collation == InvalidOid)
					state = FDW_COLLATE_NONE;
				else if (inner_cxt.state == FDW_COLLATE_SAFE &&
						 collation == inner_cxt.collation)
					state = FDW_COLLATE_SAFE;
				else if (collation == DEFAULT_COLLATION_OID)
					state = FDW_COLLATE_NONE;
				else
					state = FDW_COLLATE_UNSAFE;
			}
			break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not built-in, it can't be sent to
	 * remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !mysql_is_builtin(exprType(node)))
		return false;

	/*
	 * Now, merge my collation information into my parent's state.
	 */
	if (state > outer_cxt->state)
	{
		/* Override previous parent state */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* Merge, or detect error if there's a collation conflict */
		switch (state)
		{
			case FDW_COLLATE_NONE:
				/* Nothing + nothing is still nothing */
				break;
			case FDW_COLLATE_SAFE:
				if (collation != outer_cxt->collation)
				{
					/*
					 * Non-default collation always beats default.
					 */
					if (outer_cxt->collation == DEFAULT_COLLATION_OID)
					{
						/* Override previous parent state */
						outer_cxt->collation = collation;
					}
					else if (collation != DEFAULT_COLLATION_OID)
					{
						/*
						 * Conflict; show state as indeterminate.  We don't
						 * want to "return false" right away, since parent
						 * node might not care about collation.
						 */
						outer_cxt->state = FDW_COLLATE_UNSAFE;
					}
				}
				break;
			case FDW_COLLATE_UNSAFE:
				/* We're still conflicted ... */
				break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool
mysql_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr,
					  bool is_remote_cond)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) (baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.is_remote_cond = is_remote_cond;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	if (!foreign_expr_walker((Node *) expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (contain_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * mysql_append_conditions
 * 		Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed.  This function is used
 * to deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos
 * or bare clauses.
 */
static void
mysql_append_conditions(List *exprs, deparse_expr_cxt *context)
{
	ListCell   *lc;
	bool		is_first = true;
	StringInfo	buf = context->buf;

	foreach(lc, exprs)
	{
		Expr	   *expr = (Expr *) lfirst(lc);

		/*
		 * Extract clause from RestrictInfo, if required. See comments in
		 * declaration of MySQLFdwRelationInfo for details.
		 */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) expr;

			expr = ri->clause;
		}

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}
}

/*
 * mysql_deparse_from_expr_for_rel
 * 		Construct a FROM clause
 */
void
mysql_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
								RelOptInfo *foreignrel, bool use_alias,
								List **params_list)
{
	MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) foreignrel->fdw_private;

	if (IS_JOIN_REL(foreignrel))
	{
		RelOptInfo *rel_o = fpinfo->outerrel;
		RelOptInfo *rel_i = fpinfo->innerrel;
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;

		/* Deparse outer relation */
		initStringInfo(&join_sql_o);
		mysql_deparse_from_expr_for_rel(&join_sql_o, root, rel_o, true,
										params_list);

		/* Deparse inner relation */
		initStringInfo(&join_sql_i);
		mysql_deparse_from_expr_for_rel(&join_sql_i, root, rel_i, true,
										params_list);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * ((outer relation) <join type> (inner relation) ON (joinclauses)
		 */
		appendStringInfo(buf, "(%s %s JOIN %s ON ", join_sql_o.data,
						 mysql_get_jointype_name(fpinfo->jointype),
						 join_sql_i.data);

		/* Append join clause; (TRUE) if no join clause */
		if (fpinfo->joinclauses)
		{
			deparse_expr_cxt context;

			context.buf = buf;
			context.foreignrel = foreignrel;
			context.scanrel = foreignrel;
			context.root = root;
			context.params_list = params_list;
			context.is_not_distinct_op = false;

			appendStringInfo(buf, "(");
			mysql_append_conditions(fpinfo->joinclauses, &context);
			appendStringInfo(buf, ")");
		}
		else
			appendStringInfoString(buf, "(TRUE)");

		/* End the FROM clause entry. */
		appendStringInfo(buf, ")");
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);
		Relation	rel;

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
#if PG_VERSION_NUM < 130000
		rel = heap_open(rte->relid, NoLock);
#else
		rel = table_open(rte->relid, NoLock);
#endif

		mysql_deparse_relation(buf, rel);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX,
							 foreignrel->relid);

#if PG_VERSION_NUM < 130000
		heap_close(rel, NoLock);
#else
		table_close(rel, NoLock);
#endif
	}
	return;
}

/*
 * mysql_get_jointype_name
 * 		Output join name for given join type
 */
extern const char *
mysql_get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * mysql_append_function_name
 *		Deparses function name from given function oid.
 */
static void
mysql_append_function_name(Oid funcid, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	HeapTuple	proctup;
	Form_pg_proc procform;
	const char *proname;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Always print the function name */
	proname = NameStr(procform->proname);

	/* Translate PostgreSQL function into MySQL function */
	proname = mysql_replace_function(NameStr(procform->proname));

	appendStringInfoString(buf, proname);

	ReleaseSysCache(proctup);
}

/*
 * mysql_deparse_aggref
 *		Deparse an Aggref node.
 */
static void
mysql_deparse_aggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	/* Only basic, non-split aggregation accepted. */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* Find aggregate name from aggfnoid which is a pg_proc entry. */
	mysql_append_function_name(node->aggfnoid, context);
	appendStringInfoChar(buf, '(');

	/* Add DISTINCT */
	appendStringInfoString(buf, (node->aggdistinct != NIL) ? "DISTINCT " : "");

	/* aggstar can be set only in zero-argument aggregates. */
	if (node->aggstar)
		appendStringInfoChar(buf, '*');
	else
	{
		ListCell   *arg;
		bool		first = true;

		/* Add all the arguments. */
		foreach(arg, node->args)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(arg);
			Node	   *n = (Node *) tle->expr;

			if (tle->resjunk)
				continue;

			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			deparseExpr((Expr *) n, context);
		}
	}

	appendStringInfoChar(buf, ')');
}

/*
 * mysql_append_groupby_clause
 * 		Deparse GROUP BY clause.
 */
static void
mysql_append_groupby_clause(List *tlist, deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	Query	   *query = context->root->parse;
	ListCell   *lc;
	bool		first = true;

	/* Nothing to be done, if there's no GROUP BY clause in the query. */
	if (!query->groupClause)
		return;

	appendStringInfoString(buf, " GROUP BY ");

	/*
	 * Queries with grouping sets are not pushed down, so we don't expect
	 * grouping sets here.
	 */
	Assert(!query->groupingSets);

	foreach(lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *) lfirst(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		mysql_deparse_sort_group_clause(grp->tleSortGroupRef, tlist,
										true, context);
	}
}

/*
 * mysql_deparse_sort_group_clause
 * 		Appends a sort or group clause.
 */
static Node *
mysql_deparse_sort_group_clause(Index ref, List *tlist, bool force_colno,
								deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;
	TargetEntry *tle;
	Expr	   *expr;

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (force_colno)
	{
		/* Use column-number form when requested by caller. */
		Assert(!tle->resjunk);
		appendStringInfo(buf, "%d", tle->resno);
	}
	else if (expr && IsA(expr, Const))
	{
		/*
		 * Force a typecast here so that we don't emit something like "GROUP
		 * BY 2", which will be misconstrued as a column position rather than
		 * a constant.
		 */
		mysql_deparse_const((Const *) expr, context);
	}
	else if (!expr || IsA(expr, Var))
		deparseExpr(expr, context);
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');
	}

	return (Node *) expr;
}

/*
 * mysql_is_foreign_param
 * 		Returns true if given expr is something we'd have to send the
 * 		value of to the foreign server.
 *
 * This should return true when the expression is a shippable node that
 * deparseExpr would add to context->params_list.  Note that we don't care
 * if the expression *contains* such a node, only whether one appears at top
 * level.  We need this to detect cases where setrefs.c would recognize a
 * false match between an fdw_exprs item (which came from the params_list)
 * and an entry in fdw_scan_tlist (which we're considering putting the given
 * expression into).
 */
bool
mysql_is_foreign_param(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Expr *expr)
{
	if (expr == NULL)
		return false;

	switch (nodeTag(expr))
	{
		case T_Var:
			{
				/* It would have to be sent unless it's a foreign Var. */
				Var		   *var = (Var *) expr;
				Relids		relids;
				MySQLFdwRelationInfo *fpinfo = (MySQLFdwRelationInfo *) (baserel->fdw_private);

				if (IS_UPPER_REL(baserel))
					relids = fpinfo->outerrel->relids;
				else
					relids = baserel->relids;

				if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
					return false;	/* foreign Var, so not a param. */
				else
					return true;	/* it'd have to be a param. */
				break;
			}
		case T_Param:
			/* Params always have to be sent to the foreign server. */
			return true;
		default:
			break;
	}
	return false;
}

/*
 * mysql_append_orderby_clause
 *		Deparse ORDER BY clause defined by the given pathkeys.
 *
 * The clause should use Vars from context->scanrel if !has_final_sort, or from
 * context->foreignrel's targetlist if has_final_sort.
 *
 * We find a suitable pathkey expression (some earlier step should have
 * verified that there is one) and deparse it.
 */
void
mysql_append_orderby_clause(List *pathkeys, bool has_final_sort,
							deparse_expr_cxt *context)
{
	ListCell   *lcell;
	char	   *delim = " ";
	StringInfo	buf = context->buf;

	appendStringInfo(buf, " ORDER BY");
	foreach(lcell, pathkeys)
	{
		PathKey    *pathkey = lfirst(lcell);
		EquivalenceMember *em;
		Expr	   *em_expr;
		char	   *sortby_dir = NULL;

		if (has_final_sort)
		{
			/*
			 * By construction, context->foreignrel is the input relation to
			 * the final sort.
			 */
			em = mysql_find_em_for_rel_target(context->root,
											  pathkey->pk_eclass,
											  context->foreignrel);
		}
		else
			em = mysql_find_em_for_rel(context->root,
									   pathkey->pk_eclass,
									   context->scanrel);

		/*
		 * We don't expect any error here; it would mean that shippability
		 * wasn't verified earlier.  For the same reason, we don't recheck
		 * shippability of the sort operator.
		 */
		if (em == NULL)
			elog(ERROR, "could not find pathkey item to sort");

		em_expr = em->em_expr;

		sortby_dir = mysql_get_sortby_direction_string(em, pathkey);

		appendStringInfoString(buf, delim);

		mysql_append_orderby_suffix(em_expr, sortby_dir,
									exprType((Node *) em_expr),
									pathkey->pk_nulls_first, context);

		delim = ", ";
	}
}

/*
 * mysql_append_limit_clause
 * 		Deparse LIMIT OFFSET clause.
 */
static void
mysql_append_limit_clause(deparse_expr_cxt *context)
{
	PlannerInfo *root = context->root;
	StringInfo	buf = context->buf;

	if (root->parse->limitCount)
	{
		Const	   *c = (Const *) root->parse->limitOffset;

		appendStringInfoString(buf, " LIMIT ");
		deparseExpr((Expr *) root->parse->limitCount, context);

		if (c && !c->constisnull)
		{
			appendStringInfoString(buf, " OFFSET ");
			deparseExpr((Expr *) c, context);
		}
	}
}

#if PG_VERSION_NUM >= 140000
/*
 * mysql_deparse_truncate_sql
 * 		Construct a simple "TRUNCATE <relname>" statement.
 */
void
mysql_deparse_truncate_sql(StringInfo buf, Relation rel)
{
	appendStringInfoString(buf, "TRUNCATE ");

	mysql_deparse_relation(buf, rel);
}
#endif

/*
 * mysql_append_orderby_suffix
 * 		Append the ASC/DESC and NULLS FIRST/LAST parts of an ORDER BY clause.
 */
static void
mysql_append_orderby_suffix(Expr *em_expr, const char *sortby_dir,
							Oid sortcoltype, bool nulls_first,
							deparse_expr_cxt *context)
{
	StringInfo	buf = context->buf;

	Assert(sortby_dir != NULL);

	deparseExpr(em_expr, context);

	/*
	 * Since MySQL doesn't have NULLS FIRST/LAST clause, we use IS NOT NULL or
	 * IS NULL instead.
	 */
	if (nulls_first)
		appendStringInfoString(buf, " IS NOT NULL");
	else
		appendStringInfoString(buf, " IS NULL");

	/* Add delimiter */
	appendStringInfoString(buf, ", ");

	deparseExpr(em_expr, context);

	/* Add the ASC/DESC */
	appendStringInfo(buf, " %s", sortby_dir);
}

/*
 * mysql_is_foreign_pathkey
 *		Returns true if it's safe to push down the sort expression described by
 *		'pathkey' to the foreign server.
 */
bool
mysql_is_foreign_pathkey(PlannerInfo *root, RelOptInfo *baserel,
						 PathKey *pathkey)
{
	EquivalenceMember *em = NULL;
	EquivalenceClass *pathkey_ec = pathkey->pk_eclass;

	/*
	 * mysql_is_foreign_expr would detect volatile expressions as well, but
	 * checking ec_has_volatile here saves some cycles.
	 */
	if (pathkey_ec->ec_has_volatile)
		return false;

	/* can push if a suitable EC member exists */
	em = mysql_find_em_for_rel(root, pathkey_ec, baserel);

	if (mysql_get_sortby_direction_string(em, pathkey) == NULL)
		return false;

	return true;
}
