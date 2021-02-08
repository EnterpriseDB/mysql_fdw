\set MYSQL_HOST			'\'localhost\''
\set MYSQL_PORT			'\'3306\''
\set MYSQL_USER_NAME	'\'edb\''
\set MYSQL_PASS			'\'edb\''

-- Before running this file User must create database mysql_fdw_regress on
-- mysql with all permission for 'edb' user with 'edb' password and ran
-- mysql_init.sh file to create tables.

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
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1;

-- INNER JOIN with where condition.  Should execute where condition separately
-- on remote side.
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;

-- INNER JOIN in which join clause is not pushable.
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (abs(t1.c1) = t2.c1) WHERE t1.c2 = 100
  ORDER BY t1.c3, t1.c1;

-- Join three tables
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c2, t3.c3
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) JOIN fdw139_t3 t3 ON (t3.c1 = t1.c1)
  ORDER BY t1.c3, t1.c1;
SELECT t1.c1, t2.c2, t3.c3
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1) JOIN fdw139_t3 t3 ON (t3.c1 = t1.c1)
  ORDER BY t1.c3, t1.c1;

-- LEFT OUTER JOIN
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1 NULLS LAST;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1 NULLS LAST;

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
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 RIGHT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t2.c1, t1.c1 NULLS LAST;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 RIGHT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t2.c1, t1.c1 NULLS LAST;

-- Combinations of various joins
-- INNER JOIN + RIGHT JOIN
-- target list order is different for v10 and v96.
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
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE OF t1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE OF t1;

-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR UPDATE;

-- Join two tables with FOR SHARE clause
-- target list order is different for v10 and v96.
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR SHARE OF t1;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c3, t1.c1 FOR SHARE OF t1;

-- target list order is different for v10 and v96.
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
  ORDER BY t1.c1 LIMIT 10;
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c1)
  ORDER BY t1.c1 LIMIT 10;

-- ANTI JOIN, not pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE NOT EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c2)
  ORDER BY t1.c1 LIMIT 10;
SELECT t1.c1
  FROM fdw139_t1 t1 WHERE NOT EXISTS (SELECT 1 FROM fdw139_t2 t2 WHERE t1.c1 = t2.c2)
  ORDER BY t1.c1 LIMIT 10;

-- CROSS JOIN can be pushed down
EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2
  ORDER BY t1.c1, t2.c1 LIMIT 10;
SELECT t1.c1, t2.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2
  ORDER BY t1.c1, t2.c1 LIMIT 10;

-- CROSS JOIN combined with local table.
CREATE TABLE local_t1(c1 int);
INSERT INTO local_t1 VALUES (1), (2);

EXPLAIN (COSTS false, VERBOSE)
SELECT t1.c1, t2.c1, l1.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2 CROSS JOIN local_t1 l1
  ORDER BY t1.c1, t2.c1, l1.c1 LIMIT 10;
SELECT t1.c1, t2.c1, l1.c1
  FROM fdw139_t1 t1 CROSS JOIN fdw139_t2 t2 CROSS JOIN local_t1 l1
  ORDER BY t1.c1, t2.c1, l1.c1 LIMIT 10;
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
-- target list order is different for v10 and v96.
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
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;  -- not pushed down, different view owners
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;

ALTER VIEW v1 OWNER TO regress_view_owner;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;  -- pushed down
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN v2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;  -- not pushed down, view owner not current user
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;

ALTER VIEW v1 OWNER TO CURRENT_USER;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;  -- pushed down
SELECT t1.c1, t2.c2
  FROM v1 t1 LEFT JOIN fdw139_t2 t2 ON (t1.c1 = t2.c1)
  ORDER BY t1.c1, t2.c1, t2.c2 NULLS LAST LIMIT 10;
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

-- Cleanup
DROP OWNED BY regress_view_owner;
DROP ROLE regress_view_owner;
DELETE FROM fdw139_t1;
DELETE FROM fdw139_t2;
DELETE FROM fdw139_t3;
DELETE FROM fdw139_t4;
DROP FOREIGN TABLE fdw139_t1;
DROP FOREIGN TABLE fdw139_t2;
DROP FOREIGN TABLE fdw139_t3;
DROP FOREIGN TABLE fdw139_t4;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP USER MAPPING FOR public SERVER mysql_svr1;
DROP SERVER mysql_svr;
DROP SERVER mysql_svr1;
DROP EXTENSION mysql_fdw;
