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

-- Create foreign tables
CREATE FOREIGN TABLE f_test_tbl1 (c1 INTEGER, c2 VARCHAR(10), c3 CHAR(9), c4 BIGINT, c5 pg_catalog.Date, c6 DECIMAL, c7 INTEGER, c8 SMALLINT)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'test_tbl1');
CREATE FOREIGN TABLE f_test_tbl2 (c1 INTEGER, c2 VARCHAR(14), c3 VARCHAR(13))
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'test_tbl2');

-- Insert data in mysql db using foreign tables
INSERT INTO f_test_tbl1 VALUES (100, 'EMP1', 'ADMIN', 1300, '1980-12-17', 800.23, NULL, 20);
INSERT INTO f_test_tbl1 VALUES (200, 'EMP2', 'SALESMAN', 600, '1981-02-20', 1600.00, 300, 30);
INSERT INTO f_test_tbl1 VALUES (300, 'EMP3', 'SALESMAN', 600, '1981-02-22', 1250, 500, 30);
INSERT INTO f_test_tbl1 VALUES (400, 'EMP4', 'MANAGER', 900, '1981-04-02', 2975.12, NULL, 20);
INSERT INTO f_test_tbl1 VALUES (500, 'EMP5', 'SALESMAN', 600, '1981-09-28', 1250, 1400, 30);
INSERT INTO f_test_tbl1 VALUES (600, 'EMP6', 'MANAGER', 900, '1981-05-01', 2850, NULL, 30);
INSERT INTO f_test_tbl1 VALUES (700, 'EMP7', 'MANAGER', 900, '1981-06-09', 2450.45, NULL, 10);
INSERT INTO f_test_tbl1 VALUES (800, 'EMP8', 'FINANCE', 400, '1987-04-19', 3000, NULL, 20);
INSERT INTO f_test_tbl1 VALUES (900, 'EMP9', 'HEAD', NULL, '1981-11-17', 5000, NULL, 10);
INSERT INTO f_test_tbl1 VALUES (1000, 'EMP10', 'SALESMAN', 600, '1980-09-08', 1500, 0, 30);
INSERT INTO f_test_tbl1 VALUES (1100, 'EMP11', 'ADMIN', 800, '1987-05-23', 1100, NULL, 20);
INSERT INTO f_test_tbl1 VALUES (1200, 'EMP12', 'ADMIN', 600, '1981-12-03', 950, NULL, 30);
INSERT INTO f_test_tbl1 VALUES (1300, 'EMP13', 'FINANCE', 400, '1981-12-03', 3000, NULL, 20);
INSERT INTO f_test_tbl1 VALUES (1400, 'EMP14', 'ADMIN', 700, '1982-01-23', 1300, NULL, 10);
INSERT INTO f_test_tbl2 VALUES(10, 'DEVELOPMENT', 'PUNE');
INSERT INTO f_test_tbl2 VALUES(20, 'ADMINISTRATION', 'BANGLORE');
INSERT INTO f_test_tbl2 VALUES(30, 'SALES', 'MUMBAI');
INSERT INTO f_test_tbl2 VALUES(40, 'HR', 'NAGPUR');

SET datestyle TO ISO;

-- WHERE clause pushdown

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6 AS "salary", c8 FROM f_test_tbl1 e
  WHERE c6 IN (800,2450)
  ORDER BY c1;
SELECT c1, c2, c6 AS "salary", c8 FROM f_test_tbl1 e
  WHERE c6 IN (800,2450)
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl1 e
  WHERE c6 > 3000
  ORDER BY c1;
SELECT * FROM f_test_tbl1 e
  WHERE c6 > 3000
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 = 1500
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 = 1500
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 BETWEEN 1000 AND 4000
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c6 BETWEEN 1000 AND 4000
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IS NOT NULL
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IS NOT NULL
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17'
  ORDER BY c1;
SELECT * FROM f_test_tbl1 e
  WHERE c5 <= '1980-12-17'
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c2 IN ('EMP6', 'EMP12', 'EMP5')
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'SALESMAN'
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'SALESMAN'
  ORDER BY c1;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'MANA%'
  ORDER BY c1;
SELECT c1, c2, c6, c8 FROM f_test_tbl1 e
  WHERE c3 LIKE 'MANA%'
  ORDER BY c1;


-- FDW-516: IS [NOT] DISTINCT FROM clause should deparse correctly.

CREATE FOREIGN TABLE f_distinct_test (id int, c1 int, c2 int, c3 text, c4 text)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'distinct_test');
INSERT INTO f_distinct_test VALUES
  (1, 1, 1, 'abc', 'abc'),
  (2, 2, NULL, 'abc', 'NULL'),
  (3, NULL, NULL, 'NULL', 'NULL'),
  (4, 3, 4, 'abc', 'pqr'),
  (5, 4, 5, 'abc', 'abc'),
  (6, 5, 5, 'abc', 'pqr');
SELECT * FROM f_distinct_test ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test WHERE (c1) IS DISTINCT FROM (c2)
  ORDER BY id;
SELECT * FROM f_distinct_test WHERE (c1) IS DISTINCT FROM (c2)
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test WHERE (c1) IS NOT DISTINCT FROM (c2)
  ORDER BY id;
SELECT * FROM f_distinct_test WHERE (c1) IS NOT DISTINCT FROM (c2)
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test WHERE (c3) IS DISTINCT FROM (c4)
  ORDER BY id;
SELECT * FROM f_distinct_test WHERE (c3) IS DISTINCT FROM (c4)
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test WHERE (c3) IS NOT DISTINCT FROM (c4)
  ORDER BY id;
SELECT * FROM f_distinct_test WHERE (c3) IS NOT DISTINCT FROM (c4)
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE (c1) IS DISTINCT FROM (c2) and (c3) IS NOT DISTINCT FROM (c4)
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE (c1) IS DISTINCT FROM (c2) and (c3) IS NOT DISTINCT FROM (c4)
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE (c1) IS NOT DISTINCT FROM (c2) or (c3) IS DISTINCT FROM (c4)
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE (c1) IS NOT DISTINCT FROM (c2) or (c3) IS DISTINCT FROM (c4)
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE ((c1) IS NOT DISTINCT FROM (c2)) IS DISTINCT FROM ((c3) IS NOT DISTINCT FROM (c4))
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE ((c1) IS NOT DISTINCT FROM (c2)) IS DISTINCT FROM ((c3) IS NOT DISTINCT FROM (c4))
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE ((c1) IS DISTINCT FROM (c2)) IS NOT DISTINCT FROM ((c3) IS DISTINCT FROM (c4))
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE ((c1) IS DISTINCT FROM (c2)) IS NOT DISTINCT FROM ((c3) IS DISTINCT FROM (c4))
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE ((c1) IS NOT DISTINCT FROM (c2)) IS DISTINCT FROM ((c3) IS DISTINCT FROM (c4))
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE ((c1) IS NOT DISTINCT FROM (c2)) IS DISTINCT FROM ((c3) IS DISTINCT FROM (c4))
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE ((c1) IS DISTINCT FROM (c2)) IS NOT DISTINCT FROM ((c3) IS NOT DISTINCT FROM (c4))
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE ((c1) IS DISTINCT FROM (c2)) IS NOT DISTINCT FROM ((c3) IS NOT DISTINCT FROM (c4))
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE ((c1) IS DISTINCT FROM (c2)) IS DISTINCT FROM ((c3) IS NOT DISTINCT FROM (c4))
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE ((c1) IS DISTINCT FROM (c2)) IS DISTINCT FROM ((c3) IS NOT DISTINCT FROM (c4))
  ORDER BY id;

EXPLAIN (VERBOSE, COSTS FALSE)
SELECT * FROM f_distinct_test
  WHERE ((c1) IS NOT DISTINCT FROM (c2)) IS NOT DISTINCT FROM ((c3) IS DISTINCT FROM (c4))
  ORDER BY id;
SELECT * FROM f_distinct_test
  WHERE ((c1) IS NOT DISTINCT FROM (c2)) IS NOT DISTINCT FROM ((c3) IS DISTINCT FROM (c4))
  ORDER BY id;

-- Cleanup
DELETE FROM f_test_tbl1;
DELETE FROM f_test_tbl2;
DELETE FROM f_distinct_test;
DROP FOREIGN TABLE f_test_tbl1;
DROP FOREIGN TABLE f_test_tbl2;
DROP FOREIGN TABLE f_distinct_test;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP SERVER mysql_svr;
DROP EXTENSION mysql_fdw;
