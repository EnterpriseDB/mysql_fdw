\set MYSQL_HOST			`echo \'"$MYSQL_HOST"\'`
\set MYSQL_PORT			`echo \'"$MYSQL_PORT"\'`
\set MYSQL_USER_NAME	`echo \'"$MYSQL_USER_NAME"\'`
\set MYSQL_PASS			`echo \'"$MYSQL_PWD"\'`

-- Before running this file User must create database mysql_fdw_regress on
-- MySQL with all permission for MYSQL_USER_NAME user with MYSQL_PWD password
-- and ran mysql_init.sh file to create tables.

\c contrib_regression
CREATE EXTENSION IF NOT EXISTS mysql_fdw;
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);
CREATE USER MAPPING FOR public SERVER mysql_svr
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);

-- Validate extension, server and mapping details
CREATE OR REPLACE FUNCTION show_details(host TEXT, port TEXT, uid TEXT, pwd TEXT) RETURNS int AS $$
DECLARE
  ext TEXT;
  srv TEXT;
  sopts TEXT;
  uopts TEXT;
BEGIN
  SELECT e.fdwname, srvname, array_to_string(s.srvoptions, ','), array_to_string(u.umoptions, ',')
    INTO ext, srv, sopts, uopts
    FROM pg_foreign_data_wrapper e LEFT JOIN pg_foreign_server s ON e.oid = s.srvfdw LEFT JOIN pg_user_mapping u ON s.oid = u.umserver
    WHERE e.fdwname = 'mysql_fdw'
    ORDER BY 1, 2, 3, 4;

  raise notice 'Extension            : %', ext;
  raise notice 'Server               : %', srv;

  IF strpos(sopts, host) <> 0 AND strpos(sopts, port) <> 0 THEN
    raise notice 'Server_Options       : matched';
  END IF;

  IF strpos(uopts, uid) <> 0 AND strpos(uopts, pwd) <> 0 THEN
    raise notice 'User_Mapping_Options : matched';
  END IF;

  return 1;
END;
$$ language plpgsql;

SELECT show_details(:MYSQL_HOST, :MYSQL_PORT, :MYSQL_USER_NAME, :MYSQL_PASS);

-- Create foreign table and perform basic SQL operations
CREATE FOREIGN TABLE f_mysql_test(a int, b int)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'mysql_test');
SELECT a, b FROM f_mysql_test ORDER BY 1, 2;
INSERT INTO f_mysql_test (a, b) VALUES (2, 2);
SELECT a, b FROM f_mysql_test ORDER BY 1, 2;
UPDATE f_mysql_test SET b = 3 WHERE a = 2;
SELECT a, b FROM f_mysql_test ORDER BY 1, 2;
DELETE FROM f_mysql_test WHERE a = 2;
SELECT a, b FROM f_mysql_test ORDER BY 1, 2;

DROP FOREIGN TABLE f_mysql_test;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP SERVER mysql_svr;

-- Server with init_command.
CREATE SERVER mysql_svr1 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT, init_command 'create table init_command_check(a int)');
CREATE USER MAPPING FOR public SERVER mysql_svr1
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);
CREATE FOREIGN TABLE f_mysql_test (a int, b int)
  SERVER mysql_svr1 OPTIONS (dbname 'mysql_fdw_regress', table_name 'mysql_test');
-- This will create init_command_check table in mysql_fdw_regress database.
SELECT a, b FROM f_mysql_test ORDER BY 1, 2;

-- init_command_check table created mysql_fdw_regress database can be verified
-- by creating corresponding foreign table here.
CREATE FOREIGN TABLE f_init_command_check(a int)
  SERVER mysql_svr1 OPTIONS (dbname 'mysql_fdw_regress', table_name 'init_command_check');
SELECT a FROM f_init_command_check ORDER BY 1;
-- Changing init_command to drop init_command_check table from
-- mysql_fdw_regress database
ALTER SERVER mysql_svr1 OPTIONS (SET init_command 'drop table init_command_check');
SELECT a, b FROM f_mysql_test;

DROP FOREIGN TABLE f_init_command_check;
DROP FOREIGN TABLE f_mysql_test;
DROP USER MAPPING FOR public SERVER mysql_svr1;
DROP SERVER mysql_svr1;

-- Server with use_remote_estimate.
CREATE SERVER mysql_svr1 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS(host :MYSQL_HOST, port :MYSQL_PORT, use_remote_estimate 'TRUE');
CREATE USER MAPPING FOR public SERVER mysql_svr1
  OPTIONS(username :MYSQL_USER_NAME, password :MYSQL_PASS);
CREATE FOREIGN TABLE f_mysql_test(a int, b int)
  SERVER mysql_svr1 OPTIONS(dbname 'mysql_fdw_regress', table_name 'mysql_test');

-- Below explain will return actual rows from MySQL, but keeping costs off
-- here for consistent regression result.
EXPLAIN (VERBOSE, COSTS OFF) SELECT a FROM f_mysql_test WHERE a < 2 ORDER BY 1;

DROP FOREIGN TABLE f_mysql_test;
DROP USER MAPPING FOR public SERVER mysql_svr1;
DROP SERVER mysql_svr1;

-- Create server with secure_auth.
CREATE SERVER mysql_svr1 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS(host :MYSQL_HOST, port :MYSQL_PORT, secure_auth 'FALSE');
CREATE USER MAPPING FOR public SERVER mysql_svr1
  OPTIONS(username :MYSQL_USER_NAME, password :MYSQL_PASS);
CREATE FOREIGN TABLE f_mysql_test(a int, b int)
  SERVER mysql_svr1 OPTIONS(dbname 'mysql_fdw_regress', table_name 'mysql_test');

-- Below should fail with Warning of secure_auth is false.
SELECT a, b FROM f_mysql_test ORDER BY 1, 2;
DROP FOREIGN TABLE f_mysql_test;
DROP USER MAPPING FOR public SERVER mysql_svr1;
DROP SERVER mysql_svr1;

-- FDW-335: Support for fetch_size option at server level and table level.
CREATE SERVER fetch101 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS( fetch_size '101' );

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'fetch101'
  AND srvoptions @> array['fetch_size=101'];

ALTER SERVER fetch101 OPTIONS( SET fetch_size '202' );

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'fetch101'
  AND srvoptions @> array['fetch_size=101'];

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'fetch101'
  AND srvoptions @> array['fetch_size=202'];

CREATE FOREIGN TABLE table30000 ( x int ) SERVER fetch101
  OPTIONS ( fetch_size '30000' );

SELECT COUNT(*)
  FROM pg_foreign_table
  WHERE ftrelid = 'table30000'::regclass
  AND ftoptions @> array['fetch_size=30000'];

ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '60000');

SELECT COUNT(*)
  FROM pg_foreign_table
  WHERE ftrelid = 'table30000'::regclass
  AND ftoptions @> array['fetch_size=30000'];

SELECT COUNT(*)
  FROM pg_foreign_table
  WHERE ftrelid = 'table30000'::regclass
  AND ftoptions @> array['fetch_size=60000'];

-- Make sure that changing the table level fetch-size value did not change the
-- server level value.
SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'fetch101'
  AND srvoptions @> array['fetch_size=202'];

-- Negative test cases for fetch_size option, should error out.
ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '-60000');
ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '123abc');
ALTER FOREIGN TABLE table30000 OPTIONS ( SET fetch_size '999999999999999999999');

-- Cleanup fetch_size test objects.
DROP FOREIGN TABLE  table30000;
DROP SERVER fetch101;

-- FDW-350: Support for reconnect option at server level.
CREATE SERVER reconnect1 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS( reconnect 'true' );

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'reconnect1'
  AND srvoptions @> array['reconnect=true'];

ALTER SERVER reconnect1 OPTIONS( SET reconnect 'false' );

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'reconnect1'
  AND srvoptions @> array['reconnect=false'];

-- Negative test case for reconnect option, should error out.
ALTER SERVER reconnect1 OPTIONS ( SET reconnect 'abc1' );

-- Cleanup reconnect option test objects.
DROP SERVER reconnect1;

-- FDW-404: Support for character_set option at server level.
CREATE SERVER charset101 FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS( character_set 'utf8' );

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'charset101'
  AND srvoptions @> array['character_set=utf8'];

ALTER SERVER charset101 OPTIONS( SET character_set 'latin' );

SELECT count(*)
  FROM pg_foreign_server
  WHERE srvname = 'charset101'
  AND srvoptions @> array['character_set=latin'];

-- Cleanup character_set test objects.
DROP SERVER charset101;

-- Cleanup
DROP EXTENSION mysql_fdw;
