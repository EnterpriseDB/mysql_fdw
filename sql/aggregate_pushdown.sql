\set MYSQL_HOST			`echo \'"$MYSQL_HOST"\'`
\set MYSQL_PORT			`echo \'"$MYSQL_PORT"\'`
\set MYSQL_USER_NAME	`echo \'"$MYSQL_USER_NAME"\'`
\set MYSQL_PASS			`echo \'"$MYSQL_PWD"\'`

-- Before running this file User must create database mysql_fdw_regress on
-- mysql with all permission for MYSQL_USER_NAME user with MYSQL_PWD password
-- and ran mysql_init.sh file to create tables.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mysql_fdw;

-- FDW-132: Support for aggregate pushdown.
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);
CREATE USER MAPPING FOR public SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

CREATE TYPE user_enum AS ENUM ('foo', 'bar', 'buz');
CREATE FOREIGN TABLE fdw132_t1(c1 int, c2 int, c3 text COLLATE "C", c4 text COLLATE "C")
  SERVER mysql_svr OPTIONS(dbname 'mysql_fdw_regress', table_name 'test1');
CREATE FOREIGN TABLE fdw132_t2(c1 int, c2 int, c3 text COLLATE "C", c4 text COLLATE "C")
  SERVER mysql_svr OPTIONS(dbname 'mysql_fdw_regress', table_name 'test2');

INSERT INTO fdw132_t1 values(1, 100, 'AAA1', 'foo');
INSERT INTO fdw132_t1 values(2, 100, 'AAA2', 'bar');
INSERT INTO fdw132_t1 values(11, 100, 'AAA11', 'foo');

INSERT INTO fdw132_t2 values(1, 200, 'BBB1', 'foo');
INSERT INTO fdw132_t2 values(2, 200, 'BBB2', 'bar');
INSERT INTO fdw132_t2 values(12, 200, 'BBB12', 'foo');

ALTER FOREIGN TABLE fdw132_t1 ALTER COLUMN c4 type user_enum;
ALTER FOREIGN TABLE fdw132_t2 ALTER COLUMN c4 type user_enum;

-- Simple aggregates
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1), avg(c1), min(c2), max(c1), sum(c1) * (random() <= 1)::int AS sum2 FROM fdw132_t1 WHERE c2 > 5 GROUP BY c2 ORDER BY 1, 2;
SELECT sum(c1), avg(c1), min(c2), max(c1), sum(c1) * (random() <= 1)::int AS sum2 FROM fdw132_t1 WHERE c2 > 5 GROUP BY c2 ORDER BY 1, 2;

-- Aggregate is not pushed down as aggregation contains random()
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1 * (random() <= 1)::int) AS sum, avg(c1) FROM fdw132_t1;
SELECT sum(c1 * (random() <= 1)::int) AS sum, avg(c1) FROM fdw132_t1;

-- Aggregate over join query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), sum(t1.c1), avg(t2.c1) FROM  fdw132_t1 t1 INNER JOIN fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c1 = 2;
SELECT count(*), sum(t1.c1), avg(t2.c1) FROM  fdw132_t1 t1 INNER JOIN fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c1 = 2;

-- Not pushed down due to local conditions present in underneath input rel
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(t1.c1), count(t2.c1) FROM fdw132_t1 t1 INNER JOIN fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;
SELECT sum(t1.c1), count(t2.c1) FROM fdw132_t1 t1 INNER JOIN fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE ((t1.c1 * t2.c1)/(t1.c1 * t2.c1)) * random() <= 1;

-- GROUP BY clause HAVING expressions
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2+2, sum(c2) * (c2+2) FROM fdw132_t1 GROUP BY c2+2 ORDER BY c2+2;
SELECT c2+2, sum(c2) * (c2+2) FROM fdw132_t1 GROUP BY c2+2 ORDER BY c2+2;

-- Aggregates in subquery are pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(x.a), sum(x.a) FROM (SELECT c2 a, sum(c1) b FROM fdw132_t1 GROUP BY c2 ORDER BY 1, 2) x;
SELECT count(x.a), sum(x.a) FROM (SELECT c2 a, sum(c1) b FROM fdw132_t1 GROUP BY c2 ORDER BY 1, 2) x;

-- Aggregate is still pushed down by taking unshippable expression out
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2 * (random() <= 1)::int AS sum1, sum(c1) * c2 AS sum2 FROM fdw132_t1 GROUP BY c2 ORDER BY 1, 2;
SELECT c2 * (random() <= 1)::int AS sum1, sum(c1) * c2 AS sum2 FROM fdw132_t1 GROUP BY c2 ORDER BY 1, 2;

-- Aggregate with unshippable GROUP BY clause are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2 * (random() <= 1)::int AS c2 FROM fdw132_t2 GROUP BY c2 * (random() <= 1)::int ORDER BY 1;
SELECT c2 * (random() <= 1)::int AS c2 FROM fdw132_t2 GROUP BY c2 * (random() <= 1)::int ORDER BY 1;

-- GROUP BY clause in various forms, cardinal, alias and constant expression
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(c2) w, c2 x, 5 y, 7.0 z FROM fdw132_t1 GROUP BY 2, y, 9.0::int ORDER BY 2;
SELECT count(c2) w, c2 x, 5 y, 7.0 z FROM fdw132_t1 GROUP BY 2, y, 9.0::int ORDER BY 2;

-- Testing HAVING clause shippability
SET enable_sort TO ON;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c1) FROM fdw132_t2 GROUP BY c2 HAVING avg(c1) < 500 and sum(c1) < 49800 ORDER BY c2;
SELECT c2, sum(c1) FROM fdw132_t2 GROUP BY c2 HAVING avg(c1) < 500 and sum(c1) < 49800 ORDER BY c2;

-- Using expressions in HAVING clause
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c3, count(c1) FROM fdw132_t1 GROUP BY c3 HAVING sqrt(max(c1)) = sqrt(2) ORDER BY 1, 2;
SELECT c3, count(c1) FROM fdw132_t1 GROUP BY c3 HAVING sqrt(max(c1)) = sqrt(2) ORDER BY 1, 2;

SET enable_sort TO off;
-- Unshippable HAVING clause will be evaluated locally, and other qual in HAVING clause is pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*) FROM (SELECT c3, count(c1) FROM fdw132_t1 GROUP BY c3 HAVING (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;
SELECT count(*) FROM (SELECT c3, count(c1) FROM fdw132_t1 GROUP BY c3 HAVING (avg(c1) / avg(c1)) * random() <= 1 and avg(c1) < 500) x;

-- Aggregate in HAVING clause is not pushable, and thus aggregation is not pushed down
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1) FROM fdw132_t1 GROUP BY c2 HAVING avg(c1 * (random() <= 1)::int) > 1 ORDER BY 1;
SELECT sum(c1) FROM fdw132_t1 GROUP BY c2 HAVING avg(c1 * (random() <= 1)::int) > 1 ORDER BY 1;

-- Testing ORDER BY, DISTINCT, FILTER, Ordered-sets and VARIADIC within aggregates
-- ORDER BY within aggregates (same column used to order) are not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1 ORDER BY c1) FROM fdw132_t1 WHERE c1 < 100 GROUP BY c2 ORDER BY 1;
SELECT sum(c1 ORDER BY c1) FROM fdw132_t1 WHERE c1 < 100 GROUP BY c2 ORDER BY 1;

-- ORDER BY within aggregate (different column used to order also using DESC)
-- are not pushed.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c2 ORDER BY c1 desc) FROM fdw132_t2 WHERE c1 > 1 and c2 > 50;
SELECT sum(c2 ORDER BY c1 desc) FROM fdw132_t2 WHERE c1 > 1 and c2 > 50;

-- DISTINCT within aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT (c1)%5) FROM fdw132_t2 WHERE c2 = 200 and c1 < 50;
SELECT sum(DISTINCT (c1)%5) FROM fdw132_t2 WHERE c2 = 200 and c1 < 50;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT (t1.c1)%5) FROM fdw132_t1 t1 join fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) GROUP BY (t2.c1)%3 ORDER BY 1;
SELECT sum(DISTINCT (t1.c1)%5) FROM fdw132_t1 t1 join fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) GROUP BY (t2.c1)%3 ORDER BY 1;

-- DISTINCT with aggregate within aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(DISTINCT c1) FROM fdw132_t2 WHERE c2 = 200 and c1 < 50;
SELECT sum(DISTINCT c1) FROM fdw132_t2 WHERE c2 = 200 and c1 < 50;

-- DISTINCT combined with ORDER BY within aggregate is not pushed.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT array_agg(DISTINCT (t1.c1)%5 ORDER BY (t1.c1)%5) FROM fdw132_t1 t1 join fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) GROUP BY (t2.c1)%3 ORDER BY 1;
SELECT array_agg(DISTINCT (t1.c1)%5 ORDER BY (t1.c1)%5) FROM fdw132_t1 t1 join fdw132_t2 t2 ON (t1.c1 = t2.c1) WHERE t1.c1 < 20 or (t1.c1 is null and t2.c1 < 5) GROUP BY (t2.c1)%3 ORDER BY 1;

-- DISTINCT, ORDER BY and FILTER within aggregate, not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1%3), sum(DISTINCT c1%3 ORDER BY c1%3) filter (WHERE c1%3 < 2), c2 FROM fdw132_t1 WHERE c2 = 100 GROUP BY c2;
SELECT sum(c1%3), sum(DISTINCT c1%3 ORDER BY c1%3) filter (WHERE c1%3 < 2), c2 FROM fdw132_t1 WHERE c2 = 100 GROUP BY c2;

-- FILTER within aggregate, not pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c1) filter (WHERE c1 < 100 and c2 > 5) FROM fdw132_t1 GROUP BY c2 ORDER BY 1 nulls last;
SELECT sum(c1) filter (WHERE c1 < 100 and c2 > 5) FROM fdw132_t1 GROUP BY c2 ORDER BY 1 nulls last;

-- Outer query is aggregation query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT (SELECT count(*) filter (WHERE t2.c2 = 200 and t2.c1 < 10) FROM fdw132_t1 t1 WHERE t1.c1 = 2) FROM fdw132_t2 t2 ORDER BY 1;
SELECT DISTINCT (SELECT count(*) filter (WHERE t2.c2 = 200 and t2.c1 < 10) FROM fdw132_t1 t1 WHERE t1.c1 = 2) FROM fdw132_t2 t2 ORDER BY 1;

-- Inner query is aggregation query
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT (SELECT count(t1.c1) filter (WHERE t2.c2 = 200 and t2.c1 < 10) FROM fdw132_t1 t1 WHERE t1.c1 = 2) FROM fdw132_t2 t2 ORDER BY 1;
SELECT DISTINCT (SELECT count(t1.c1) filter (WHERE t2.c2 = 200 and t2.c1 < 10) FROM fdw132_t1 t1 WHERE t1.c1 = 2) FROM fdw132_t2 t2 ORDER BY 1;

-- Ordered-sets within aggregate, not pushed down.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, rank('10'::varchar) within group (ORDER BY c3), percentile_cont(c2/200::numeric) within group (ORDER BY c1) FROM fdw132_t2 GROUP BY c2 HAVING percentile_cont(c2/200::numeric) within group (ORDER BY c1) < 500 ORDER BY c2;
SELECT c2, rank('10'::varchar) within group (ORDER BY c3), percentile_cont(c2/200::numeric) within group (ORDER BY c1) FROM fdw132_t2 GROUP BY c2 HAVING percentile_cont(c2/200::numeric) within group (ORDER BY c1) < 500 ORDER BY c2;

-- Using multiple arguments within aggregates
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, rank(c1, c2) within group (ORDER BY c1, c2) FROM fdw132_t1 GROUP BY c1, c2 HAVING c1 = 2 ORDER BY 1;
SELECT c1, rank(c1, c2) within group (ORDER BY c1, c2) FROM fdw132_t1 GROUP BY c1, c2 HAVING c1 = 2 ORDER BY 1;

-- Subquery in FROM clause HAVING aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT count(*), x.b FROM fdw132_t1, (SELECT c1 a, sum(c1) b FROM fdw132_t2 GROUP BY c1) x WHERE fdw132_t1.c1 = x.a GROUP BY x.b ORDER BY 1, 2;
SELECT count(*), x.b FROM fdw132_t1, (SELECT c1 a, sum(c1) b FROM fdw132_t2 GROUP BY c1) x WHERE fdw132_t1.c1 = x.a GROUP BY x.b ORDER BY 1, 2;

-- Join with IS NULL check in HAVING
EXPLAIN (VERBOSE, COSTS OFF)
SELECT avg(t1.c1), sum(t2.c1) FROM fdw132_t1 t1 join fdw132_t2 t2 ON (t1.c1 = t2.c1) GROUP BY t2.c1 HAVING (avg(t1.c1) is null and sum(t2.c1) > 10) or sum(t2.c1) is null ORDER BY 1 nulls last, 2;
SELECT avg(t1.c1), sum(t2.c1) FROM fdw132_t1 t1 join fdw132_t2 t2 ON (t1.c1 = t2.c1) GROUP BY t2.c1 HAVING (avg(t1.c1) is null and sum(t2.c1) > 10) or sum(t2.c1) is null ORDER BY 1 nulls last, 2;

-- ORDER BY expression is part of the target list but not pushed down to
-- foreign server.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT sum(c2) * (random() <= 1)::int AS sum FROM fdw132_t1 ORDER BY 1;
SELECT sum(c2) * (random() <= 1)::int AS sum FROM fdw132_t1 ORDER BY 1;

-- LATERAL join, with parameterization
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum FROM fdw132_t1 t1, lateral (SELECT sum(t2.c1 + t1.c1) sum FROM fdw132_t2 t2 GROUP BY t2.c1) qry WHERE t1.c2 * 2 = qry.sum and t1.c2 > 10 ORDER BY 1;

-- Check with placeHolderVars
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q.b, count(fdw132_t1.c1), sum(q.a) FROM fdw132_t1 left join (SELECT min(13), avg(fdw132_t1.c1), sum(fdw132_t2.c1) FROM fdw132_t1 right join fdw132_t2 ON (fdw132_t1.c1 = fdw132_t2.c1) WHERE fdw132_t1.c1 = 12) q(a, b, c) ON (fdw132_t1.c1 = q.b) WHERE fdw132_t1.c1 between 10 and 15 GROUP BY q.b ORDER BY 1 nulls last, 2;
SELECT q.b, count(fdw132_t1.c1), sum(q.a) FROM fdw132_t1 left join (SELECT min(13), avg(fdw132_t1.c1), sum(fdw132_t2.c1) FROM fdw132_t1 right join fdw132_t2 ON (fdw132_t1.c1 = fdw132_t2.c1) WHERE fdw132_t1.c1 = 12) q(a, b, c) ON (fdw132_t1.c1 = q.b) WHERE fdw132_t1.c1 between 10 and 15 GROUP BY q.b ORDER BY 1 nulls last, 2;

-- Not supported cases
-- Grouping sets
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c1) FROM fdw132_t1 WHERE c2 > 3 GROUP BY rollup(c2) ORDER BY 1 nulls last;
SELECT c2, sum(c1) FROM fdw132_t1 WHERE c2 > 3 GROUP BY rollup(c2) ORDER BY 1 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c1) FROM fdw132_t1 WHERE c2 > 3 GROUP BY cube(c2) ORDER BY 1 nulls last;
SELECT c2, sum(c1) FROM fdw132_t1 WHERE c2 > 3 GROUP BY cube(c2) ORDER BY 1 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, c3, sum(c1) FROM fdw132_t1 WHERE c2 > 3 GROUP BY grouping sets(c2, c3) ORDER BY 1 nulls last, 2 nulls last;
SELECT c2, c3, sum(c1) FROM fdw132_t1 WHERE c2 > 3 GROUP BY grouping sets(c2, c3) ORDER BY 1 nulls last, 2 nulls last;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c1), grouping(c2) FROM fdw132_t1 WHERE c2 > 3 GROUP BY c2 ORDER BY 1 nulls last;
SELECT c2, sum(c1), grouping(c2) FROM fdw132_t1 WHERE c2 > 3 GROUP BY c2 ORDER BY 1 nulls last;

-- DISTINCT itself is not pushed down, whereas underneath aggregate is pushed
EXPLAIN (VERBOSE, COSTS OFF)
SELECT DISTINCT sum(c1) s FROM fdw132_t1 WHERE c2 > 6 GROUP BY c2 ORDER BY 1;
SELECT DISTINCT sum(c1) s FROM fdw132_t1 WHERE c2 > 6 GROUP BY c2 ORDER BY 1;

-- WindowAgg
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, sum(c2), count(c2) over (partition by c2%2) FROM fdw132_t1 WHERE c2 > 10 GROUP BY c2 ORDER BY 1;
SELECT c2, sum(c2), count(c2) over (partition by c2%2) FROM fdw132_t1 WHERE c2 > 10 GROUP BY c2 ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, array_agg(c2) over (partition by c2%2 ORDER BY c2 desc) FROM fdw132_t1 WHERE c2 > 10 GROUP BY c2 ORDER BY 1;
SELECT c2, array_agg(c2) over (partition by c2%2 ORDER BY c2 desc) FROM fdw132_t1 WHERE c2 > 10 GROUP BY c2 ORDER BY 1;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, array_agg(c2) over (partition by c2%2 ORDER BY c2 range between current row and unbounded following) FROM fdw132_t1 WHERE c2 > 10 GROUP BY c2 ORDER BY 1;
SELECT c2, array_agg(c2) over (partition by c2%2 ORDER BY c2 range between current row and unbounded following) FROM fdw132_t1 WHERE c2 > 10 GROUP BY c2 ORDER BY 1;

-- User defined function for user defined aggregate, VARIADIC
CREATE FUNCTION least_accum(anyelement, variadic anyarray)
returns anyelement language sql AS
  'SELECT least($1, min($2[i])) FROM generate_subscripts($2,2) g(i)';
CREATE aggregate least_agg(variadic items anyarray) (
  stype = anyelement, sfunc = least_accum
);
-- Not pushed down due to user defined aggregate
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, least_agg(c1) FROM fdw132_t1 GROUP BY c2 ORDER BY c2;
SELECT c2, least_agg(c1) FROM fdw132_t1 GROUP BY c2 ORDER BY c2;

-- FDW-129: Limit and offset pushdown with Aggregate pushdown.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT 1 OFFSET 1;
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT 1 OFFSET 1;

-- Limit 0, Offset 0 with aggregates.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT 0 OFFSET 0;
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT 0 OFFSET 0;

-- Limit NULL
EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT NULL OFFSET 2;
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT NULL OFFSET 2;

-- Limit ALL
EXPLAIN (VERBOSE, COSTS OFF)
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT ALL OFFSET 2;
SELECT min(c1), c1 FROM fdw132_t1 GROUP BY c1 ORDER BY c1 LIMIT ALL OFFSET 2;

-- Delete existing data and load new data for partition-wise aggregate test
-- cases.
DELETE FROM fdw132_t1;
DELETE FROM fdw132_t2;
INSERT INTO fdw132_t1 values(1, 1, 'AAA1', 'foo');
INSERT INTO fdw132_t1 values(2, 2, 'AAA2', 'bar');
INSERT INTO fdw132_t2 values(3, 3, 'AAA11', 'foo');
INSERT INTO fdw132_t2 values(4, 4, 'AAA12', 'foo');

-- Test partition-wise aggregates
SET enable_partitionwise_aggregate TO on;

-- Create the partition table.
CREATE TABLE fprt1 (c1 int, c2 int, c3 varchar, c4 varchar) PARTITION BY RANGE(c1);
CREATE FOREIGN TABLE ftprt1_p1 PARTITION OF fprt1 FOR VALUES FROM (1) TO (2)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'test1');
CREATE FOREIGN TABLE ftprt1_p2 PARTITION OF fprt1 FOR VALUES FROM (3) TO (4)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', TABLE_NAME 'test2');

-- Plan with partitionwise aggregates is enabled
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c1) FROM fprt1 GROUP BY c1 ORDER BY 2;
SELECT c1, sum(c1) FROM fprt1 GROUP BY c1 ORDER BY 2;

EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, sum(c2), min(c2), count(*) FROM fprt1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 2;
SELECT c1, sum(c2), min(c2), count(*) FROM fprt1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;

-- Check with whole-row reference
-- Should have all the columns in the target list for the given relation
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c1, count(t1) FROM fprt1 t1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;
SELECT c1, count(t1) FROM fprt1 t1 GROUP BY c1 HAVING avg(c2) < 22 ORDER BY 1;

-- When GROUP BY clause does not match with PARTITION KEY.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT c2, avg(c1), max(c1), count(*) FROM fprt1 GROUP BY c2 HAVING sum(c1) < 700 ORDER BY 1;
SELECT c2, avg(c1), max(c1), count(*) FROM fprt1 GROUP BY c2 HAVING sum(c1) < 700 ORDER BY 1;

SET enable_partitionwise_aggregate TO off;

-- Cleanup
DROP aggregate least_agg(variadic items anyarray);
DROP FUNCTION least_accum(anyelement, variadic anyarray);
DELETE FROM fdw132_t1;
DELETE FROM fdw132_t2;
DROP FOREIGN TABLE fdw132_t1;
DROP FOREIGN TABLE fdw132_t2;
DROP FOREIGN TABLE ftprt1_p1;
DROP FOREIGN TABLE ftprt1_p2;
DROP TABLE IF EXISTS fprt1;
DROP TYPE user_enum;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP SERVER mysql_svr;
DROP EXTENSION mysql_fdw;
