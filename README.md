MySQL Foreign Data Wrapper for PostgreSQL
=========================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [MySQL][1] and some versions of [MariaDB](https://mariadb.org/).

<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" align="center" height="100" alt="PostgreSQL"/>	+	<img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/MariaDB_colour_logo.svg" align="center" height="100" alt="MariaDB"/></br></br>
<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" align="center" height="100" alt="PostgreSQL"/>	+	<img src="https://upload.wikimedia.org/wikipedia/ru/d/d3/Mysql.png" align="center" height="100" alt="MySQL"/>

Supports PostgreSQL and EDB Postgres Advanced Server 10, 11, 12, 13, 14 and 15.

Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Identifier case handling](#identifier-case-handling)
7. [Generated columns](#generated-columns)
8. [Character set handling](#character-set-handling)
9. [Examples](#examples)
10. [Limitations](#limitations)
11. [Contributing](#contributing)
12. [Support](#support)
13. [Useful links](#useful-links)
14. [License](#license)

Features
--------
### Common features & enhancements

The following enhancements are added to the latest version of
``mysql_fdw``:

#### Write-able FDW
The previous version was only read-only, the latest version provides the
write capability. The user can now issue an insert, update, and delete
statements for the foreign tables using the `mysql_fdw`. It uses the PG
type casting mechanism to provide opposite type casting between MySQL
and PG data types.

#### Connection Pooling
The latest version comes with a connection pooler that utilises the same
MySQL database connection for all the queries in the same session. The
previous version would open a new MySQL database connection for every
query. This is a performance enhancement.

#### Prepared Statement
(Refactoring for `select` queries to use prepared statement)

The `select` queries are now using prepared statements instead of simple
query protocol.

## Pushdowning

#### WHERE clause push-down
The latest version will push-down the foreign table where clause to
the foreign server. The where condition on the foreign table will be
executed on the foreign server hence there will be fewer rows to bring
across to PostgreSQL. This is a performance feature.

#### Column push-down
The previous version was fetching all the columns from the target
foreign table. The latest version does the column push-down and only
brings back the columns that are part of the select target list. This is
a performance feature.

#### JOIN push-down
`mysql_fdw` now also supports join push-down. The joins between two
foreign tables from the same remote MySQL server are pushed to a remote
server, instead of fetching all the rows for both the tables and
performing a join locally, thereby enhancing the performance. Currently,
joins involving only relational and arithmetic operators in join-clauses
are pushed down to avoid any potential join failure. Also, only the
INNER and LEFT/RIGHT OUTER joins are supported, and not the FULL OUTER,
SEMI, and ANTI join. This is a performance feature.

#### AGGREGATE push-down
`mysql_fdw` now also supports aggregate push-down. Push aggregates to the
remote MySQL server instead of fetching all of the rows and aggregating
them locally. This gives a very good performance boost for the cases
where aggregates can be pushed down. The push-down is currently limited
to aggregate functions min, max, sum, avg, and count, to avoid pushing
down the functions that are not present on the MySQL server. Also,
aggregate filters and orders are not pushed down.

#### ORDER BY push-down
`mysql_fdw` now also supports order by push-down. If possible, push order by
clause to the remote server so that we get the ordered result set from the
foreign server itself. It might help us to have an efficient merge join.
NULLs behavior is opposite on the MySQL server. Thus to get an equivalent
result, we add the "expression IS NULL" clause at the beginning of each of
the ORDER BY expressions.

#### LIMIT OFFSET push-down
`mysql_fdw` now also supports limit offset push-down. Wherever possible,
perform LIMIT and OFFSET operations on the remote server. This reduces
network traffic between local PostgreSQL and remote MySQL servers.
ALL/NULL options are not supported on the MySQL server, and thus they are
not pushed down. Also, OFFSET without LIMIT is not supported on the MySQL
server hence queries having that construct are not pushed.

Supported platforms
-------------------

`mysql_fdw` was developed on Linux, and should run on any
reasonably POSIX-compliant system.

`mysql_fdw` is designed to be compatible with PostgreSQL 10 ~ 15. 
Please note that this version of mysql_fdw works with PostgreSQL and EDB
Postgres Advanced Server 11, 12, 13, 14, 15, and 16.

Installation
------------
### Prerequisites

To compile the [MySQL][1] foreign data wrapper, MySQL's C client library
is needed. This library can be downloaded from the official [MySQL
website][1].

### Source installation

1. To build on POSIX-compliant systems you need to ensure the
   `pg_config` executable is in your path when you run `make`. This
   executable is typically in your PostgreSQL installation's `bin`
   directory. For example:

    ```
    $ export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. The `mysql_config` must also be in the path, it resides in the MySQL
   `bin` directory.

    ```
    $ export PATH=/usr/local/mysql/bin/:$PATH
    ```

3. Compile the code using make.

    ```
    $ make USE_PGXS=1
    ```

4.  Finally install the foreign data wrapper.

    ```
    $ make USE_PGXS=1 install
    ```

5. Running regression test.

    ```
    $ make USE_PGXS=1 installcheck
    ```
   However, make sure to set the `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER_NAME`,
   and `MYSQL_PWD` environment variables correctly. The default settings
   can be found in the `mysql_init.sh` script.

If you run into any issues, please [let us know][2].

Usage
-----

## CREATE SERVER options

`mysql_fdw` accepts the following options via the `CREATE SERVER` command:

- **host** as *string*, optional, default `127.0.0.1`

  Address or hostname of the MySQL server.
  
- **port** as *integer*, optional, default `3306`

  Port number of the MySQL server.

- **secure_auth** as *boolean*, optional, default `true`

  Enable or disable secure authentication.

- **init_command** as *string*, optional, no default

  SQL statement to execute when connecting to the
    MySQL server.

- **use_remote_estimate** as *boolean*, optional, default `false`

  Controls whether `mysql_fdw` issues remote
    `EXPLAIN` commands to obtain cost estimates.
    
- **reconnect** as *boolean*, optional, default `false`

  Enable or disable automatic reconnection to the
    MySQL server if the existing connection is found to have been lost.

- **sql_mode** as *string*, optional, default `ANSI_QUOTES`

  Set MySQL sql_mode for established connection.

- **ssl_key** as *string*, optional, no default

  The path name of the client private key file.

- **ssl_cert** as *string*, optional, no default

  The path name of the client public key certificate file.

- **ssl_ca** as *string*, optional, no default

  The path name of the Certificate Authority (CA) certificate
    file. This option, if used, must specify the same certificate used
    by the server.

- **ssl_capath** as *string*, optional, no default

  The path name of the directory that contains trusted
    SSL CA certificate files.

- **ssl_cipher** as *string*, optional, no default

  The list of permissible ciphers for SSL encryption.

- **fetch_size** as *integer*, optional, default `100`

  This option specifies the number of rows `mysql_fdw` should
    get in each fetch operation. It can be specified for a foreign table or
    a foreign server. The option specified on a table overrides an option
    specified for the server.

- **character_set** as *string*, optional, default `auto`

  The character set to use for MySQL connection. Default
    is `auto` which means autodetect based on the operating system setting.
    Before the introduction of the `character_set` option, the character set
    was set similar to the PostgreSQL database encoding. To get this older
    behavior set the `character_set` to special value `PGDatabaseEncoding`.

- **truncatable** as *boolean*, optional    

## CREATE USER MAPPING options

`mysql_fdw` accepts the following options via the `CREATE USER MAPPING`
command:

- **username** as *string*, no default

  Username to use when connecting to MySQL.

- **password** as *string*, no default

  Password to authenticate to the MySQL server with.


## CREATE FOREIGN TABLE options

`mysql_fdw` accepts the following table-level options via the
`CREATE FOREIGN TABLE` command.

- **dbname** as *string*, mandatory

  Name of the MySQL database to query. This is a mandatory
    option.
    
- **table_name** as *string*, optional, default name of foreign table

  Name of the MySQL table.

- **fetch_size** as *integer*, optional

  Same as `fetch_size` parameter for foreign server.

- **max_blob_size** as *integer*, optional

  Max blob size to read without truncation.
  
- **truncatable** as *boolean*, optional

  The same as foreign server option.

## IMPORT FOREIGN SCHEMA options

`mysql_fdw` supports [IMPORT FOREIGN SCHEMA](https://www.postgresql.org/docs/current/sql-importforeignschema.html) and 
 accepts the following custom options:
 
- **import_default** as *boolean*, optional, default `false`

  This option controls whether column DEFAULT
  expressions are included in the definitions of foreign tables imported
  from a foreign server.
  
- **import_not_null** as *boolean*, optional, default `true`

  This option controls whether column NOT NULL
  constraints are included in the definitions of foreign tables imported
  from a foreign server.
  
- **import_enum_as_text** as *boolean*, optional, default `false`

  This option can be used to map MySQL ENUM type
  to TEXT type in the definitions of foreign tables, otherwise emit a
  warning for type to be created.
  
- **import_generated** as *boolean*, optional, default `true`

  This option controls whether GENERATED column
  expressions are included in the definitions of foreign tables imported from
  a foreign server or not. The IMPORT will fail altogether if an
  imported generated expression uses a function or operator that
  does not exist on PostgreSQL.

## TRUNCATE support

`mysql_fdw` implements the foreign data wrapper `TRUNCATE` API, available
from PostgreSQL 14. MySQL/MariaDB does provide a `TRUNCATE` command, see https://mariadb.com/kb/en/truncate-table/.

Following restrictions apply:

 - `TRUNCATE ... CASCADE` is not supported
 - `TRUNCATE ... RESTART IDENTITY` is not supported
 - MySQL/MariaDB tables with foreign key references cannot be truncated

These restrictions may be removed in future releases.

Functions
---------

As well as the standard `mysql_fdw_handler()` and `mysql_fdw_validator()`
functions, `mysql_fdw` provides the following user-callable utility functions:

Functions from this FDW in PostgreSQL catalog are **yet not described**.

Identifier case handling
------------------------

As PostgreSQL and MySQL/MariaDB take opposite approaches to case folding (PostgreSQL
folds identifiers to lower case by default, MySQL/MariaDB to upper case), it's important
to be aware of potential issues with table and column names.

When defining foreign tables, PostgreSQL will pass any identifiers which do not
require quoting to MySQL/MariaDB as-is, defaulting to lower-case. MySQL/MariaDB will then
implictly fold these to upper case. For example, given the following table
definitions in MySQL/MariaDB and PostgreSQL:

    CREATE TABLE CASETEST1 (
      COL1 INT
    )

    CREATE FOREIGN TABLE casetest1 (
      col1 INT
    )
    SERVER fb_test

and given the PostgreSQL query:

    SELECT col1 FROM casetest1

`mysql_fdw` will generate the following MySQL/MariaDB query:

    SELECT col1 FROM casetest1

which is valid in both PostgreSQL and MySQL/MariaDB.

By default, PostgreSQL will pass any identifiers which do require quoting
according to PostgreSQL's definition as quoted identifiers to MySQL/MariaDB. For
example, given the following table definitions in MySQL/MariaDB and PostgreSQL:

    CREATE TABLE "CASEtest2" (
      "Col1" INT
    )

    CREATE FOREIGN TABLE "CASEtest2" (
      "Col1" INT
    )
    SERVER fb_test

and given the PostgreSQL query:

    SELECT "Col1" FROM "CASEtest2"

`mysql_fdw` will generate the following MySQL/MariaDB query:

    SELECT "Col1" FROM "CASEtest2"

which is also valid in both PostgreSQL and MySQL/MariaDB.

The same query will also be generated if the MySQL/MariaDB table and column names
are specified as options:

    CREATE FOREIGN TABLE casetest2a (
      col1 INT OPTIONS (column_name 'Col1')
    )
    SERVER fb_test
    OPTIONS (table_name 'CASEtest2')

However PostgreSQL will not quote lower-case identifiers by default. With the
following MySQL/MariaDB and PostgreSQL table definitions:

    CREATE TABLE "casetest3" (
      "col1" INT
    )

    CREATE FOREIGN TABLE "casetest3" (
      "col1" INT
    )
    SERVER fb_test

any attempt to access the foreign table `casetest3` will result in the MySQL/MariaDB
error `Table unknown: CASETEST3`, as MySQL/MariaDB is receiving the unquoted PostgreSQL
table name and folding it to upper case.

All rules and problems with MySQL/MariaDB identifiers **yet not tested and described**.

Generated columns
-----------------

Behavoiur within generated columns **yet not tested**. 

Note that while `mysql_fdw` will insert or update the generated column value
in MySQL/MariaDB, there is nothing to stop the value being modified within MySQL/MariaDB,
and hence no guarantee that in subsequent `SELECT` operations the column will
still contain the expected generated value. This limitation also applies to
`postgres_fdw`.

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)

Character set handling
----------------------

Encodings mapping between PostgeeSQL and MySQL/MariaDB **yet not described**.

Examples
--------

```sql
-- load extension first time after install
CREATE EXTENSION mysql_fdw;

-- create server object
CREATE SERVER mysql_server
	FOREIGN DATA WRAPPER mysql_fdw
	OPTIONS (host '127.0.0.1', port '3306');

-- create user mapping
CREATE USER MAPPING FOR postgres
	SERVER mysql_server
	OPTIONS (username 'foo', password 'bar');

-- create foreign table
CREATE FOREIGN TABLE warehouse
	(
		warehouse_id int,
		warehouse_name text,
		warehouse_created timestamp
	)
	SERVER mysql_server
	OPTIONS (dbname 'db', table_name 'warehouse');

-- insert new rows in table
INSERT INTO warehouse values (1, 'UPS', current_date);
INSERT INTO warehouse values (2, 'TV', current_date);
INSERT INTO warehouse values (3, 'Table', current_date);

-- select from table
SELECT * FROM warehouse ORDER BY 1;

warehouse_id | warehouse_name | warehouse_created
-------------+----------------+-------------------
           1 | UPS            | 10-JUL-20 00:00:00
           2 | TV             | 10-JUL-20 00:00:00
           3 | Table          | 10-JUL-20 00:00:00

-- delete row from table
DELETE FROM warehouse where warehouse_id = 3;

-- update a row of table
UPDATE warehouse set warehouse_name = 'UPS_NEW' where warehouse_id = 1;

-- explain a table with verbose option
EXPLAIN VERBOSE SELECT warehouse_id, warehouse_name FROM warehouse WHERE warehouse_name LIKE 'TV' limit 1;

                                   QUERY PLAN
--------------------------------------------------------------------------------------------------------------------
Limit  (cost=10.00..11.00 rows=1 width=36)
	Output: warehouse_id, warehouse_name
	->  Foreign Scan on public.warehouse  (cost=10.00..1010.00 rows=1000 width=36)
		Output: warehouse_id, warehouse_name
		Local server startup cost: 10
		Remote query: SELECT `warehouse_id`, `warehouse_name` FROM `db`.`warehouse` WHERE ((`warehouse_name` LIKE BINARY 'TV'))
```

Limitations
-----------

**Yet not described**.
   
Contributing
------------
If you experience any bug and have a fix for that, or have a new idea,
create a ticket on github page. Before creating a pull request please
read the [contributing guidelines][3].

Preferred code style see in PostgreSQL source codes. For example

```C
type
funct_name (type arg ...)
{
	t1 var1 = value1;
	t2 var2 = value2;

	for (;;)
	{
		int var ...
	}
	if ()
	{
		bool var1 ...
	}
}
```
Support
-------
This project will be modified to maintain compatibility with new
PostgreSQL and EDB Postgres Advanced Server releases.

If you require commercial support, please contact the EnterpriseDB sales
team, or check whether your existing PostgreSQL support provider can
also support `mysql_fdw`.

Useful links
------------

### Source code

Reference FDW implementation, `postgres_fdw`
 - https://git.postgresql.org/gitweb/?p=postgresql.git;a=tree;f=contrib/postgres_fdw;hb=HEAD

### General FDW Documentation

 - https://www.postgresql.org/docs/current/ddl-foreign-data.html
 - https://www.postgresql.org/docs/current/sql-createforeigndatawrapper.html
 - https://www.postgresql.org/docs/current/sql-createforeigntable.html
 - https://www.postgresql.org/docs/current/sql-importforeignschema.html
 - https://www.postgresql.org/docs/current/fdwhandler.html
 - https://www.postgresql.org/docs/current/postgres-fdw.html

### Other FDWs

 - https://wiki.postgresql.org/wiki/Fdw
 - https://pgxn.org/tag/fdw/
 
License
-------
Copyright (c) 2011-2023, EnterpriseDB Corporation.

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written
agreement is hereby granted, provided that the above copyright notice
and this paragraph and the following two paragraphs appear in all
copies.

See the [`LICENSE`][4] file for full details.

[1]: http://www.mysql.com
[2]: https://github.com/enterprisedb/`mysql_fdw`/issues/new
[3]: CONTRIBUTING.md
[4]: LICENSE
