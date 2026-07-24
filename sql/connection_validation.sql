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

-- Create foreign table and Validate
CREATE FOREIGN TABLE f_mysql_test(a int, b int)
  SERVER mysql_svr OPTIONS (dbname 'mysql_fdw_regress', table_name 'mysql_test');
SELECT * FROM f_mysql_test ORDER BY 1, 2;

-- FDW-121: After a change to a pg_foreign_server or pg_user_mapping catalog
-- entry, existing connection should be invalidated and should make new
-- connection using the updated connection details.

-- Alter SERVER option.
-- Set wrong host, subsequent operation on this server should use updated
-- details and fail as the host address is not correct. The error code in error
-- message is different for different server versions and platform, so check
-- that through plpgsql block and give the generic error message.
ALTER SERVER mysql_svr OPTIONS (SET host 'localhos');
DO
$$
BEGIN
  SELECT * FROM f_mysql_test ORDER BY 1, 2;
  EXCEPTION WHEN others THEN
	IF SQLERRM LIKE 'failed to connect to MySQL: Unknown %server host ''localhos'' (%)' THEN
	  RAISE NOTICE 'failed to connect to MySQL: Unknown MySQL server host ''localhos''';
    ELSE
	  RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;

-- Set the correct host-name, next operation should succeed.
ALTER SERVER mysql_svr OPTIONS (SET host :MYSQL_HOST);
SELECT * FROM f_mysql_test ORDER BY 1, 2;

-- Alter USER MAPPING option.
-- Set wrong password, next operation should fail.
ALTER USER MAPPING FOR PUBLIC SERVER mysql_svr
  OPTIONS (SET username :MYSQL_USER_NAME, SET password 'bar1');
DO
$$
BEGIN
  SELECT * FROM f_mysql_test ORDER BY 1, 2;
  EXCEPTION WHEN others THEN
	IF SQLERRM LIKE 'failed to connect to MySQL: Access denied for user ''%''@''%'' (using password: YES)' THEN
	  RAISE NOTICE 'failed to connect to MySQL: Access denied for MYSQL_USER_NAME';
    ELSE
	  RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;

-- Set correct user-name and password, next operation should succeed.
ALTER USER MAPPING FOR PUBLIC SERVER mysql_svr
  OPTIONS (SET username :MYSQL_USER_NAME, SET password :MYSQL_PASS);
SELECT * FROM f_mysql_test ORDER BY 1, 2;


-- FDW-654: Connection details should be fetched from mysql default file when
-- mysql_default_file server level option is set.

CREATE SERVER mysql_server FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (mysql_default_file '/tmp/my.cnf');
CREATE USER MAPPING FOR public SERVER mysql_server;
CREATE FOREIGN TABLE f_mysql_file_test(a int, b int) SERVER mysql_server
  OPTIONS (dbname 'mysql_fdw_regress', table_name 'mysql_test');

-- Negative scenario. Connection should not happen as default file has incorrect
-- details.
\! echo [client] > /tmp/my.cnf
\! echo host=localhos >> /tmp/my.cnf
\! echo user=$MYSQL_USER_NAME >> /tmp/my.cnf
\! echo password=1234 >> /tmp/my.cnf
-- Set wrong host, subsequent operation should use these connection details
-- and fail as the host address is not correct. The error code in error
-- message is different for different server versions and platform, so check
-- that through plpgsql block and give the generic error message.
DO
$$
BEGIN
  SELECT * FROM f_mysql_file_test ORDER BY 1, 2;
  EXCEPTION WHEN others THEN
	IF SQLERRM LIKE 'failed to connect to MySQL: Unknown %server host ''localhos'' (%)' THEN
	  RAISE NOTICE 'failed to connect to MySQL: Unknown MySQL server host ''localhos''';
    ELSE
	  RAISE NOTICE '%', SQLERRM;
	END IF;
END;
$$
LANGUAGE plpgsql;

-- Prepare the default file with connection details.
\! echo [client] > /tmp/my.cnf
\! echo host=$MYSQL_HOST >> /tmp/my.cnf
\! echo port=$MYSQL_PORT >> /tmp/my.cnf
\! echo user=$MYSQL_USER_NAME >> /tmp/my.cnf
\! echo password=$MYSQL_PWD >> /tmp/my.cnf
\! echo secure_auth=true >> /tmp/my.cnf
-- Connection should happen as default file exists with details.
SELECT * FROM f_mysql_file_test ORDER BY 1, 2;


-- A plan-time remote connection must use the same user mapping as execution
-- (the foreign-table RTE's checkAsUser), not the session user.  Through a view
-- owned by a role that has a mapping, a user with NO mapping of its own must
-- still be able to query: planning connects as the view owner.  (Before this was
-- fixed, planning connected as the querying user and failed with "user mapping
-- not found".)  A dedicated server with no PUBLIC mapping is used so the querying
-- role has nothing to fall back on.
CREATE SERVER mysql_svr_cau FOREIGN DATA WRAPPER mysql_fdw
  OPTIONS (host :MYSQL_HOST, port :MYSQL_PORT);
CREATE ROLE regress_cau_owner LOGIN SUPERUSER;
CREATE ROLE regress_cau_user LOGIN;
-- Only the view owner has a user mapping on this server.
CREATE USER MAPPING FOR regress_cau_owner SERVER mysql_svr_cau
  OPTIONS (username :MYSQL_USER_NAME, password :MYSQL_PASS);
CREATE FOREIGN TABLE f_cau (a int, b int)
  SERVER mysql_svr_cau OPTIONS (dbname 'mysql_fdw_regress', table_name 'mysql_test');
ALTER FOREIGN TABLE f_cau OWNER TO regress_cau_owner;
CREATE VIEW v_cau AS SELECT * FROM f_cau;
ALTER VIEW v_cau OWNER TO regress_cau_owner;
GRANT SELECT ON v_cau TO regress_cau_user;

-- Through the view, the mapping-less user succeeds: planning and execution both
-- connect as the view owner (checkAsUser).
SET ROLE regress_cau_user;
SELECT * FROM v_cau ORDER BY 1, 2;
RESET ROLE;

-- Direct access (no view, so checkAsUser is the querying user) still correctly
-- requires that user's own mapping, which does not exist.
GRANT USAGE ON FOREIGN SERVER mysql_svr_cau TO regress_cau_user;
GRANT SELECT ON f_cau TO regress_cau_user;
SET ROLE regress_cau_user;
SELECT * FROM f_cau ORDER BY 1, 2;
RESET ROLE;

-- DML through the view must resolve the view owner too: BeginForeignModify's
-- effective user comes from the result relation's permission info on PG 16+
-- (it was read from a ModifyTable node wrongly cast to ForeignScan, which
-- aliased an unrelated field and fell back to the mapping-less session user).
GRANT UPDATE ON v_cau TO regress_cau_user;
SET ROLE regress_cau_user;
UPDATE v_cau SET b = 2 WHERE a = 1;
SELECT * FROM v_cau ORDER BY 1, 2;
-- Restore the shared fixture row.  (An UPDATE through a view over the
-- PARTITIONED table would cover the ModifyTable.rootRelation aliasing
-- directly, but a cross-partition-capable UPDATE needs the tableoid system
-- attribute, which foreign scans cannot fetch, so that path is unreachable.)
UPDATE v_cau SET b = 1 WHERE a = 1;
SELECT * FROM v_cau ORDER BY 1, 2;
RESET ROLE;

-- A foreign table that is a PARTITION is an inheritance child: on PG 16+ its own
-- RTE carries no checkAsUser (perminfoindex 0), so the plan-time effective user
-- must come from the parent (RelOptInfo.userid), exactly as execution resolves
-- it.  Through a view owned by the mapped role, the mapping-less user must still
-- plan and execute as the owner.  (Before the RelOptInfo.userid fix, planning the
-- foreign partition connected as the querying user and failed with "user mapping
-- not found".)
CREATE TABLE p_cau (a int, b int) PARTITION BY RANGE (a);
CREATE FOREIGN TABLE p_cau_p1 PARTITION OF p_cau FOR VALUES FROM (MINVALUE) TO (MAXVALUE)
  SERVER mysql_svr_cau OPTIONS (dbname 'mysql_fdw_regress', table_name 'mysql_test');
ALTER TABLE p_cau OWNER TO regress_cau_owner;
ALTER FOREIGN TABLE p_cau_p1 OWNER TO regress_cau_owner;
CREATE VIEW v_pcau AS SELECT * FROM p_cau;
ALTER VIEW v_pcau OWNER TO regress_cau_owner;
GRANT SELECT ON v_pcau TO regress_cau_user;

SET ROLE regress_cau_user;
SELECT * FROM v_pcau ORDER BY 1, 2;
RESET ROLE;

DROP VIEW v_pcau;
DROP TABLE p_cau;

DROP VIEW v_cau;
DROP FOREIGN TABLE f_cau;
DROP USER MAPPING FOR regress_cau_owner SERVER mysql_svr_cau;
DROP SERVER mysql_svr_cau;
DROP ROLE regress_cau_user;
DROP ROLE regress_cau_owner;

-- Cleanup
DROP FOREIGN TABLE f_mysql_test;
DROP FOREIGN TABLE f_mysql_file_test;
DROP USER MAPPING FOR public SERVER mysql_svr;
DROP USER MAPPING FOR public SERVER mysql_server;
DROP SERVER mysql_svr;
DROP SERVER mysql_server;
\! rm -f /tmp/my.cnf
DROP EXTENSION mysql_fdw;
