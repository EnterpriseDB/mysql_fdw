MySQL Foreign Data Wrapper for PostgreSQL
=========================================

This is a foreign data wrapper (FDW) to connect [PostgreSQL](https://www.postgresql.org/)
to [MySQL][1].

Please note that this version of mysql_fdw works with PostgreSQL and EDB
Postgres Advanced Server 12, 13, 14, 15, 16, and 17.

Contents
--------

1. [Features](#features)
2. [Supported platforms](#supported-platforms)
3. [Installation](#installation)
4. [Usage](#usage)
5. [Functions](#functions)
6. [Generated columns](#generated-columns)
7. [Examples](#examples)
8. [Limitations](#limitations)
9. [Contributing](#contributing)
10. [Support](#support)
11. [Useful links](#useful-links)
12. [License](#license)

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

Please refer to [mysql_fdw_documentation][6].

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
    behavior set the character_set to special value `PGDatabaseEncoding`.

- **mysql_default_file** as string, optional, no default

  Set the MySQL default file path if connection
    details, such as username, password, etc., need to be picked from the
    default file.

- **truncatable** as *boolean*, optional, default `true`

  This option controls whether `mysql_fdw` allows foreign tables to be truncated
    using the TRUNCATE command. It can be specified for a foreign table or a
	foreign server. A table-level option overrides a server-level option.

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

- **truncatable** as *boolean*, optional, default `true`

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
from PostgreSQL 14. MySQL does provide a `TRUNCATE` command, see https://dev.mysql.com/doc/refman/8.4/en/truncate-table.html.

Following restrictions apply:

 - `TRUNCATE ... CASCADE` is not supported
 - `TRUNCATE ... RESTART IDENTITY` is not supported and ignored
 - `TRUNCATE ... CONTINUE IDENTITY` is not supported and ignored
 - MySQL tables with foreign key references cannot be truncated

Functions
---------

As well as the standard `mysql_fdw_handler()` and `mysql_fdw_validator()`
functions, `mysql_fdw` provides the following user-callable utility functions:

- **mysql_fdw_version()**

  Returns the version number as an integer.

- **mysql_fdw_display_pushdown_list()**

  Displays the `mysql_fdw_pushdown.config` file contents.


Generated columns
-----------------

Note that while `mysql_fdw` will insert or update the generated column value
in MySQL, there is nothing to stop the value being modified within MySQL,
and hence no guarantee that in subsequent `SELECT` operations the column will
still contain the expected generated value. This limitation also applies to
`postgres_fdw`.

For more details on generated columns see:

- [Generated Columns](https://www.postgresql.org/docs/current/ddl-generated-columns.html)
- [CREATE FOREIGN TABLE](https://www.postgresql.org/docs/current/sql-createforeigntable.html)


Examples
--------

### Install the extension:

Once for a database you need, as PostgreSQL superuser.

```sql
	-- load extension first time after install
	CREATE EXTENSION mysql_fdw;
```

### Create a foreign server with appropriate configuration:

Once for a foreign datasource you need, as PostgreSQL superuser.

```sql
	-- create server object
	CREATE SERVER mysql_server
	FOREIGN DATA WRAPPER mysql_fdw
	OPTIONS (host '127.0.0.1', port '3306');
```

### Grant usage on foreign server to normal user in PostgreSQL:

Once for a normal user (non-superuser) in PostgreSQL, as PostgreSQL superuser. It is a good idea to use a superuser only where really necessary, so let's allow a normal user to use the foreign server (this is not required for the example to work, but it's security recommendation).

```sql
	GRANT USAGE ON FOREIGN SERVER mysql_server TO pguser;
```
Where `pguser` is a sample user for works with foreign server (and foreign tables).

### User mapping
Create an appropriate user mapping:

```sql
	-- create user mapping
	CREATE USER MAPPING FOR pguser
	SERVER mysql_server
	OPTIONS (username 'foo', password 'bar');
```
Where `pguser` is a sample user for works with foreign server (and foreign tables).

### Create foreign table
All `CREATE FOREIGN TABLE` SQL commands can be executed as a normal PostgreSQL user if there were correct `GRANT USAGE ON FOREIGN SERVER`. No need PostgreSQL supersuer for security reasons but also works with PostgreSQL supersuer.

Please specify `table_name` option if MySQL table name is different from foreign table name.

```sql
	-- create foreign table
	CREATE FOREIGN TABLE warehouse (
	  warehouse_id int,
	  warehouse_name text,
	  warehouse_created timestamp
	)
	SERVER mysql_server
	OPTIONS (dbname 'db', table_name 'warehouse');
```
Some other operations with foreign table data

```sql
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

### Import a MySQL database as schema to PostgreSQL:

```sql
	IMPORT FOREIGN SCHEMA someschema
	FROM SERVER mysql_server
	INTO public;
```

Limitations
-----------

**Yet not described**.

For more details, please refer to [mysql_fdw documentation][5].

Contributing
------------
If you experience any bug and have a fix for that, or have a new idea,
create a ticket on github page. Before creating a pull request please
read the [contributing guidelines][3].

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
Copyright (c) 2011-2024, EnterpriseDB Corporation.

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
[5]: https://www.enterprisedb.com/docs/mysql_data_adapter/latest/
[6]: https://www.enterprisedb.com/docs/mysql_data_adapter/latest/02_requirements_overview/
