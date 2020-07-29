MySQL Foreign Data Wrapper for PostgreSQL
=========================================

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
[MySQL][1].

Please note that this version of mysql_fdw works with PostgreSQL and EDB
Postgres Advanced Server 9.5, 9.6, 10, 11, 12, and 13.

Installation
------------

To compile the [MySQL][1] foreign data wrapper, MySQL's C client library
is needed. This library can be downloaded from the official [MySQL
website][1].

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

If you run into any issues, please [let us know][2].


Enhancements
------------

The following enhancements are added to the latest version of
`mysql_fdw`:

### Write-able FDW
The previous version was only read-only, the latest version provides the
write capability. The user can now issue an insert, update, and delete
statements for the foreign tables using the mysql_fdw. It uses the PG
type casting mechanism to provide opposite type casting between MySQL
and PG data types.

### Connection Pooling
The latest version comes with a connection pooler that utilises the same
MySQL database connection for all the queries in the same session. The
previous version would open a new MySQL database connection for every
query. This is a performance enhancement.

### WHERE clause push-down
The latest version will push-down the foreign table where clause to
the foreign server. The where condition on the foreign table will be
executed on the foreign server hence there will be fewer rows to bring
across to PostgreSQL. This is a performance feature.

### Column push-down
The previous version was fetching all the columns from the target
foreign table. The latest version does the column push-down and only
brings back the columns that are part of the select target list. This is
a performance feature.

### Prepared Statement
(Refactoring for `select` queries to use prepared statement)

The `select` queries are now using prepared statements instead of simple
query protocol.

Usage
-----

The following parameters can be set on a MySQL foreign server object:

  * `host`: Address or hostname of the MySQL server. Defaults to
    `127.0.0.1`
  * `port`: Port number of the MySQL server. Defaults to `3306`
  * `secure_auth`: Enable or disable secure authentication. Default is
    `true`
  * `init_command`: SQL statement to execute when connecting to the
    MySQL server.
  * `use_remote_estimate`: Controls whether mysql_fdw issues remote
    EXPLAIN commands to obtain cost estimates. Default is `false`
  * `ssl_key`: The path name of the client private key file.
  * `ssl_cert`: The path name of the client public key certificate file.
  * `ssl_ca`: The path name of the Certificate Authority (CA) certificate
    file. This option, if used, must specify the same certificate used
    by the server.
  * `ssl_capath`: The path name of the directory that contains trusted
    SSL CA certificate files.
  * `ssl_cipher`: The list of permissible ciphers for SSL encryption.

The following parameters can be set on a MySQL foreign table object:

  * `dbname`: Name of the MySQL database to query. This is a mandatory
    option.
  * `table_name`: Name of the MySQL table, default is the same as
    foreign table.
  * `max_blob_size`: Max blob size to read without truncation.

The following parameters need to supplied while creating user mapping.

  * `username`: Username to use when connecting to MySQL.
  * `password`: Password to authenticate to the MySQL server with.

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
also support mysql_fdw.


License
-------
Copyright (c) 2011-2020, EnterpriseDB Corporation.

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written
agreement is hereby granted, provided that the above copyright notice
and this paragraph and the following two paragraphs appear in all
copies.

See the [`LICENSE`][4] file for full details.

[1]: http://www.mysql.com
[2]: https://github.com/enterprisedb/mysql_fdw/issues/new
[3]: CONTRIBUTING.md
[4]: LICENSE
