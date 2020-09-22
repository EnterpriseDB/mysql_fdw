/*-------------------------------------------------------------------------
 *
 * mysql_fdw--1.1.sql
 * 			Foreign-data wrapper for remote MySQL servers
 *
 * Portions Copyright (c) 2012-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2004-2020, EnterpriseDB Corporation.
 *
 * IDENTIFICATION
 * 			mysql_fdw--1.1.sql
 *
 *-------------------------------------------------------------------------
 */


CREATE FUNCTION mysql_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION mysql_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER mysql_fdw
  HANDLER mysql_fdw_handler
  VALIDATOR mysql_fdw_validator;

CREATE OR REPLACE FUNCTION mysql_fdw_version()
  RETURNS pg_catalog.int4 STRICT
  AS 'MODULE_PATHNAME' LANGUAGE C;
