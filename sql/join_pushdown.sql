\set MYSQL_HOST			`echo \'"$MYSQL_HOST"\'`
\set MYSQL_PORT			`echo \'"$MYSQL_PORT"\'`
\set MYSQL_USER_NAME	`echo \'"$MYSQL_USER_NAME"\'`
\set MYSQL_PASS			`echo \'"$MYSQL_PWD"\'`

-- Before running this file User must create database mysql_fdw_regress on
-- mysql with all permission for MYSQL_USER_NAME user with MYSQL_PWD password
-- and ran mysql_init.sh file to create tables.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mysql_fdw;

-- FDW-139: Support for JOIN pushdown.
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);
CREATE USER MAPPING FOR public SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

CREATE SERVER mysql_svr1 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);
CREATE USER MAPPING FOR public SERVER mysql_svr1
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
CREATE FOREIGN TABLE fdw139_t1(c1 int, c2 int, c3 text COLLATE "C", c4 text COLLATE "C")
  SERVER mysql_svr OPTIONS(dbname 'mysql_fdw_regress', table_name 'test1');
CREATE FOREIGN TABLE fdw139_t2(c1 int, c2 int, c3 text COLLATE "C", c4 text COLLATE "C")
  SERVER mysql_svr OPTIONS(dbname 'mysql_fdw_regress', table_name 'test2');
CREATE FOREIGN TABLE fdw139_t3(c1 int, c2 int, c3 text COLLATE "C")
  SERVER mysql_svr OPTIONS(dbname 'mysql_fdw_regress', table_name 'test3');
CREATE FOREIGN TABLE fdw139_t4(c1 int, c2 int, c3 text COLLATE "C")
  SERVER mysql_svr1 OPTIONS(dbname 'mysql_fdw_regress', table_name 'test3');

INSERT INTO fdw139_t1 values(1, 100, 'AAA1', 'foo');
INSERT INTO fdw139_t1 values(2, 100, 'AAA2', 'bar');
INSERT INTO fdw139_t1 values(11, 100, 'AAA11', 'foo');

INSERT INTO fdw139_t2 values(1, 200, 'BBB1', 'foo');
INSERT INTO fdw139_t2 values(2, 200, 'BBB2', 'bar');
INSERT INTO fdw139_t2 values(12, 200, 'BBB12', 'foo');

INSERT INTO fdw139_t3 values(1, 300, 'CCC1');
INSERT INTO fdw139_t3 values(2, 300, 'CCC2');
INSERT INTO fdw139_t3 values(13, 300, 'CCC13');

SET enable_mergejoin TO off;
SET enable_hashjoin TO off;
SET enable_sort TO off;

ALTER FOREIGN TABLE fdw139_t1 ALTER COLUMN c4 type user_enum;
ALTER FOREIGN TABLE fdw139_t2 ALTER COLUMN c4 type user_enum;

-- Join two tables
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;

-- INNER JOIN with where condition.  Should execute where condition separately
-- on remote side.
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;

-- INNER JOIN in which join clause is not pushable.
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;

-- Join three tables
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c2, t3.c3
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) JOIN fdw139_t3 t3 ON (t3.c1 = t1.c1)
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c2, t3.c3
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) JOIN fdw139_t3 t3 ON (t3.c1 = t1.c1)
  ORDER BY t1.c3, t1.c1;

EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1, t3.c1
  FROM fdw139_t1 t1, fdw139_t2 t2, fdw139_t3 t3 WHERE t1.c1 = 11 AND t2.c1 = 12 AND t3.c1 = 13
  ORDER BY t1.c1;

SELECT t1.c1, t2.c1, t3.c1
  FROM fdw139_t1 t1, fdw139_t2 t2, fdw139_t3 t3 WHERE t1.c1 = 11 AND t2.c1 = 12 AND t3.c1 = 13
  ORDER BY t1.c1;

-- LEFT OUTER JOIN
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1 NULLS LAST LIMIT 2 OFFSET 2;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1 NULLS LAST LIMIT 2 OFFSET 2;

-- LEFT JOIN evaluating as INNER JOIN, having unsafe join clause.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1)
  WHERE t2.c1 > 1 ORDER BY t1.c1, t2.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1)
  WHERE t2.c1 > 1 ORDER BY t1.c1, t2.c1;

-- LEFT OUTER JOIN in which join clause is not pushable.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1)
  ORDER BY t1.c1, t2.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1)
  ORDER BY t1.c1, t2.c1;

-- LEFT OUTER JOIN + placement of clauses.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t1.c2, t2.c1, t2.c2
  FROM fdw139_t1 t1 LEFT JOIN (SELECT * FROM fdw139_t2 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
  WHERE t1.c1 < 10;
SELECT t1.c1, t1.c2, t2.c1, t2.c2
  FROM fdw139_t1 t1 LEFT JOIN (SELECT * FROM fdw139_t2 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
  WHERE t1.c1 < 10;

-- Clauses within the nullable side are not pulled up, but the top level clause
-- on nullable side is not pushed down into nullable side
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t1.c2, t2.c1, t2.c2
  FROM fdw139_t1 t1 LEFT JOIN (SELECT * FROM fdw139_t2 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
  WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;
SELECT t1.c1, t1.c2, t2.c1, t2.c2
  FROM fdw139_t1 t1 LEFT JOIN (SELECT * FROM fdw139_t2 WHERE c1 < 10) t2 ON (t1.c1 = t2.c1)
  WHERE (t2.c1 < 10 OR t2.c1 IS NULL) AND t1.c1 < 10;

-- RIGHT OUTER JOIN
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 RIGHT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t2.c1, t1.c1 NULLS LAST;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 RIGHT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t2.c1, t1.c1 NULLS LAST;

-- Combinations of various joins
-- INNER JOIN + RIGHT JOIN
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c2, t3.c3
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN fdw139_t3 t3 ON (t1.c1 = t3.c1)
  ORDER BY t1.c1 NULLS LAST, t1.c3, t1.c1;
SELECT t1.c1, t2.c2, t3.c3
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) RIGHT JOIN fdw139_t3 t3 ON (t1.c1 = t3.c1)
  ORDER BY t1.c1 NULLS LAST, t1.c3, t1.c1;

-- FULL OUTER JOIN, should not be pushdown as target database doesn't support
-- it.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 FULL JOIN fdw139_t1 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 FULL JOIN fdw139_t1 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1;

-- Join two tables with FOR UPDATE clause
-- tests whole-row reference for row marks
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE OF t1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE OF t1;

-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE;

-- Join two tables with FOR SHARE clause
-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR SHARE OF t1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR SHARE OF t1;

-- target list order is different for v10.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR SHARE;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR SHARE;

-- Join in CTE.
-- Explain plan difference between v11 (or pre) and later.
EXPLAIN (COSTS false, VERBOSE)
WITH t (c1_1, c1_3, c2_1) AS (
  SELECT t1.c1, t1.c3, t2.c1
    FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1;
WITH t (c1_1, c1_3, c2_1) AS (
  SELECT t1.c1, t1.c3, t2.c1
    FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
) SELECT c1_1, c2_1 FROM t ORDER BY c1_3, c1_1;

-- Whole-row reference
EXPLAIN (COSTS false, VERBOSE)
SELECT t1, t2, t1.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;
SELECT t1, t2, t1.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;

-- SEMI JOIN, not pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c1)
  ORDER BY t1.c1;
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c1)
  ORDER BY t1.c1;

-- ANTI JOIN, not pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE NOT EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c2)
  ORDER BY t1.c1 LIMIT 2;
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE NOT EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c2)
  ORDER BY t1.c1 LIMIT 2;

-- CROSS JOIN can be pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2
  ORDER BY t1.c1, t2.c1 LIMIT round(5.4) OFFSET 2;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2
  ORDER BY t1.c1, t2.c1 LIMIT round(5.4) OFFSET 2;

-- CROSS JOIN combined with local table.
CREATE TABLE local_t1(c1 int);
INSERT INTO local_t1 VALUES (1), (2);

EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1, l1.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2 CROSS JOIN local_t1 l1
  ORDER BY t1.c1, t2.c1, l1.c1 LIMIT 8 OFFSET round(2.2);
SELECT t1.c1, t2.c1, l1.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2 CROSS JOIN local_t1 l1
  ORDER BY t1.c1, t2.c1, l1.c1 LIMIT 8 OFFSET round(2.2);
SELECT count(t1.c1)
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2 CROSS JOIN local_t1 l1;

-- Join two tables from two different foreign table
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t4 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t4 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;

-- Unsafe join conditions (c4 has a UDT), not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c4 = t2.c4)
  ORDER BY t1.c1, t2.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c4 = t2.c4)
  ORDER BY t1.c1, t2.c1;

-- Unsafe conditions on one side (c4 has a UDT), not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c4 = 'foo'
  ORDER BY t1.c1, t2.c1 NULLS LAST;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c4 = 'foo'
  ORDER BY t1.c1, t2.c1 NULLS LAST;

-- Join where unsafe to pushdown condition in WHERE clause has a column not
-- in the SELECT clause.  In this test unsafe clause needs to have column
-- references from both joining sides so that the clause is not pushed down
-- into one of the joining sides.
-- target list order is different for v10.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c4 = t2.c4
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c4 = t2.c4
  ORDER BY t1.c3, t1.c1;

-- Check join pushdown in situations where multiple userids are involved
CREATE ROLE regress_view_owner SUPERUSER;
CREATE USER MAPPING FOR regress_view_owner
  SERVER mysql_svr OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);
GRANT SELECT ON fdw139_t1 TO regress_view_owner;
GRANT SELECT ON fdw139_t2 TO regress_view_owner;

CREATE VIEW v1 AS SELECT * FROM fdw139_t1;
CREATE VIEW v2 AS SELECT * FROM fdw139_t2;
ALTER VIEW v2 OWNER TO regress_view_owner;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;  -- not pushed down, different view owners
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;

ALTER VIEW v1 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;  -- pushed down
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;  -- not pushed down, view owner not current user
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;

ALTER VIEW v1 OWNER TO CURRENT_USER;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;  -- pushed down
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST;
ALTER VIEW v1 OWNER TO regress_view_owner;

-- Non-Var items in targetlist of the nullable rel of a join preventing
-- push-down in some cases
-- Unable to push {fdw139_t1, fdw139_t2}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.a, fdw139_t2.c1
  FROM (SELECT 13 FROM fdw139_t1 WHERE c1 = 13) q(a) RIGHT JOIN fdw139_t2 ON (q.a = fdw139_t2.c1)
  WHERE fdw139_t2.c1 BETWEEN 10 AND 15;
SELECT q.a, fdw139_t2.c1
  FROM (SELECT 13 FROM fdw139_t1 WHERE c1 = 13) q(a) RIGHT JOIN fdw139_t2 ON (q.a = fdw139_t2.c1)
  WHERE fdw139_t2.c1 BETWEEN 10 AND 15;

-- Ok to push {fdw139_t1, fdw139_t2 but not {fdw139_t1, fdw139_t2, fdw139_t3}
EXPLAIN (VERBOSE, COSTS OFF)
SELECT fdw139_t3.c1, q.*
  FROM fdw139_t3 LEFT JOIN (
    SELECT 13, fdw139_t1.c1, fdw139_t2.c1
    FROM fdw139_t1 RIGHT JOIN fdw139_t2 ON (fdw139_t1.c1 = fdw139_t2.c1)
    WHERE fdw139_t1.c1 = 11
  ) q(a, b, c) ON (fdw139_t3.c1 = q.b)
  WHERE fdw139_t3.c1 BETWEEN 10 AND 15;
SELECT fdw139_t3.c1, q.*
  FROM fdw139_t3 LEFT JOIN (
    SELECT 13, fdw139_t1.c1, fdw139_t2.c1
    FROM fdw139_t1 RIGHT JOIN fdw139_t2 ON (fdw139_t1.c1 = fdw139_t2.c1)
    WHERE fdw139_t1.c1 = 11
  ) q(a, b, c) ON (fdw139_t3.c1 = q.b)
  WHERE fdw139_t3.c1 BETWEEN 10 AND 15;

-- FDW-129: Limit and offset pushdown with join pushdown.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT round(2.2) OFFSET 2;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT round(2.2) OFFSET 2;

-- Limit as NULL, no LIMIT/OFFSET pushdown.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT NULL OFFSET 1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT NULL OFFSET 1;

-- Limit as ALL, no LIMIT/OFFSET pushdown.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT ALL OFFSET 1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT ALL OFFSET 1;


-- Offset as NULL, no LIMIT/OFFSET pushdown.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT 3 OFFSET NULL;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (TRUE)
  ORDER BY t1.c1, t2.c1 LIMIT 3 OFFSET NULL;

-- Delete existing data and load new data for partition-wise join test cases.
DROP OWNED BY regress_view_owner;
DROP ROLE regress_view_owner;
DELETE FROM fdw139_t1;
DELETE FROM fdw139_t2;
DELETE FROM fdw139_t3;
INSERT INTO fdw139_t1 values(1, 1, 'AAA1', 'foo');
INSERT INTO fdw139_t1 values(2, 2, 'AAA2', 'bar');
INSERT INTO fdw139_t1 values(3, 3, 'AAA11', 'foo');
INSERT INTO fdw139_t1 values(4, 4, 'AAA12', 'foo');

INSERT INTO fdw139_t2 values(5, 5, 'BBB1', 'foo');
INSERT INTO fdw139_t2 values(6, 6, 'BBB2', 'bar');
INSERT INTO fdw139_t2 values(7, 7, 'BBB11', 'foo');
INSERT INTO fdw139_t2 values(8, 8, 'BBB12', 'foo');

INSERT INTO fdw139_t3 values(1, 1, 'CCC1');
INSERT INTO fdw139_t3 values(2, 2, 'CCC2');
INSERT INTO fdw139_t3 values(3, 3, 'CCC13');
INSERT INTO fdw139_t3 values(4, 4, 'CCC14');
DROP FOREIGN TABLE fdw139_t4;
CREATE FOREIGN TABLE tmp_t4(c1 int, c2 int, c3 text)
  SERVER mysql_svr1 OPTIONS(dbname 'mysql_fdw_regress', table_name 'test4');
INSERT INTO tmp_t4 values(5, 5, 'CCC1');
INSERT INTO tmp_t4 values(6, 6, 'CCC2');
INSERT INTO tmp_t4 values(7, 7, 'CCC13');
INSERT INTO tmp_t4 values(8, 8, 'CCC13');

-- Test partition-wise join
SET enable_partitionwise_join TO on;

-- Create the partition table.
CREATE TABLE fprt1 (c1 int, c2 int, c3 varchar, c4 varchar) PARTITION BY RANGE(c1);
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (1) TO (4)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'test1');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (5) TO (8)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', TABLE_NAME 'test2');

CREATE TABLE fprt2 (c1 int, c2 int, c3 varchar) PARTITION BY RANGE(c2);
CREATE FOREIGN TABLE ftprt2_p1 PARTITION OF fprt2 FOR VALUES FROM (1) TO (4)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'test3');
CREATE FOREIGN TABLE ftprt2_p2 PARTITION OF fprt2 FOR VALUES FROM (5) TO (8)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', TABLE_NAME 'test4');

-- Inner join three tables
-- Different explain plan on v10 as partition-wise join is not supported there.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1,t2.c2,t3.c3
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) INNER JOIN fprt1 t3 ON (t2.c2 = t3.c1)
  WHERE t1.c1 % 2 =0 ORDER BY 1,2,3;
SELECT t1.c1,t2.c2,t3.c3
  FROM fprt1 t1 INNER JOIN fprt2 t2 ON (t1.c1 = t2.c2) INNER JOIN fprt1 t3 ON (t2.c2 = t3.c1)
  WHERE t1.c1 % 2 =0 ORDER BY 1,2,3;

-- With whole-row reference; partitionwise join does not apply
-- Table alias in foreign scan is different for v12, v11 and v10.
EXPLAIN (VERBOSE, COSTS false)
SELECT t1, t2, t1.c1
  FROM fprt1 t1 JOIN fprt2 t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c3, t1.c1;
SELECT t1, t2, t1.c1
  FROM fprt1 t1 JOIN fprt2 t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c3, t1.c1;

-- Join with lateral reference
-- Different explain plan on v10 as partition-wise join is not supported there.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1,t1.c2
  FROM fprt1 t1, LATERAL (SELECT t2.c1, t2.c2 FROM fprt2 t2
  WHERE t1.c1 = t2.c2 AND t1.c2 = t2.c1) q WHERE t1.c1 % 2 = 0 ORDER BY 1,2;
SELECT t1.c1,t1.c2
  FROM fprt1 t1, LATERAL (SELECT t2.c1, t2.c2 FROM fprt2 t2
  WHERE t1.c1 = t2.c2 AND t1.c2 = t2.c1) q WHERE t1.c1 % 2 = 0 ORDER BY 1,2;

-- With PHVs, partitionwise join selected but no join pushdown
-- Table alias in foreign scan is different for v12, v11 and v10.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t1.phv, t2.c2, t2.phv
  FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE c1 % 2 = 0) t1 LEFT JOIN
    (SELECT 't2_phv' phv, * FROM fprt2 WHERE c2 % 2 = 0) t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c1, t2.c2;
SELECT t1.c1, t1.phv, t2.c2, t2.phv
  FROM (SELECT 't1_phv' phv, * FROM fprt1 WHERE c1 % 2 = 0) t1 LEFT JOIN
    (SELECT 't2_phv' phv, * FROM fprt2 WHERE c2 % 2 = 0) t2 ON (t1.c1 = t2.c2)
  ORDER BY t1.c1, t2.c2;

SET enable_partitionwise_join TO off;

-- Cleanup
DELETE FROM fdw139_t1;
DELETE FROM fdw139_t2;
DELETE FROM fdw139_t3;
DELETE FROM tmp_t4;
DROP FOREIGN TABLE fdw139_t1;
DROP FOREIGN TABLE fdw139_t2;
DROP FOREIGN TABLE fdw139_t3;
DROP FOREIGN TABLE tmp_t4;
DROP TABLE IF EXISTS fprt1;
DROP TABLE IF EXISTS fprt2;
DROP TYPE user_enum;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP USER MAPPING FOR public SERVER mysql_svr1;
DROP SERVER mysql_svr;
DROP SERVER mysql_svr1;
DROP EXTENSION mysql_fdw;
