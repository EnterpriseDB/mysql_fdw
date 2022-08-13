\set MYSQL_HOST			`echo \'"$MYSQL_HOST"\'`
\set MYSQL_PORT			`echo \'"$MYSQL_PORT"\'`
\set MYSQL_USER_NAME	`echo \'"$MYSQL_USER_NAME"\'`
\set MYSQL_PASS			`echo \'"$MYSQL_PWD"\'`

-- Before running this file User must create database mysql_fdw_regress on
-- mysql with all permission for MYSQL_USER_NAME user with MYSQL_PWD password
-- and ran mysql_init.sh file to create tables.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mysql_fdw;
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);
CREATE USER MAPPING FOR public SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

-- Create foreign table
CREATE FOREIGN TABLE f_test_tbl2 (c1 INTEGER, c2 VARCHAR(14), c3 VARCHAR(13))
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'test_tbl2');

INSERT INTO f_test_tbl2 VALUES(10, 'DEVELOPMENT', 'PUNE');
INSERT INTO f_test_tbl2 VALUES(20, 'ADMINISTRATION', 'BANGLORE');
INSERT INTO f_test_tbl2 VALUES(30, 'SALES', 'MUMBAI');
INSERT INTO f_test_tbl2 VALUES(40, 'HR', 'NAGPUR');
INSERT INTO f_test_tbl2 VALUES(50, 'IT', 'PUNE');
INSERT INTO f_test_tbl2 VALUES(60, 'DB SERVER', 'PUNE');

SELECT * FROM f_test_tbl2 ORDER BY 1;

-- LIMIT/OFFSET pushdown.
-- Limit with Offset should get pushdown.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 3 OFFSET 2;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 3 OFFSET 2;

-- Only Limit should get pushdown.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 3;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 3;

-- Expression in Limit clause.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT round(3.2) OFFSET 2;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT round(3.2) OFFSET 2;

-- Only Offset without Limit should not get pushdown.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 OFFSET 2;
SELECT * FROM f_test_tbl2 ORDER BY 1 OFFSET 2;

-- Limit ALL
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT ALL;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT ALL;

-- Limit NULL
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT NULL;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT NULL;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT NULL OFFSET 2;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT NULL OFFSET 2;

-- Limit 0 and Offset 0
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 0;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 0;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 0 OFFSET 0;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 0 OFFSET 0;

-- Offset NULL.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 3 OFFSET NULL;
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT 3 OFFSET NULL;

-- Limit with placeholder.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT (SELECT COUNT(*) FROM f_test_tbl2);
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT (SELECT COUNT(*) FROM f_test_tbl2);

-- Limit with expression, should not pushdown.
EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT (10 - (SELECT COUNT(*) FROM f_test_tbl2));
SELECT * FROM f_test_tbl2 ORDER BY 1 LIMIT (10 - (SELECT COUNT(*) FROM f_test_tbl2));

DELETE FROM f_test_tbl2;
DROP FOREIGN TABLE f_test_tbl2;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP SERVER mysql_svr;
DROP EXTENSION mysql_fdw;
