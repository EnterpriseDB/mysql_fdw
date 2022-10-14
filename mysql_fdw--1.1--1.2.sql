/* mysql_fdw/mysql_fdw--1.1--1.2.sql */

CREATE OR REPLACE FUNCTION mysql_fdw_display_pushdown_list(IN reload boolean DEFAULT false,
  OUT object_type text,
  OUT object_name text)
RETURNS SETOF record
  AS 'MODULE_PATHNAME', 'mysql_display_pushdown_list'
LANGUAGE C PARALLEL SAFE;
