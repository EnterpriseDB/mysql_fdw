MySQL Foreign Data Wrapper for PostgreSQL
=========================================

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for
[MySQL][1].

Please note that this version of mysql_fdw only works with PostgreSQL Version 9.3 and greater, for previous version support please download from PG_92 branch.

We have added a number of significant enhancements to the mysql fdw, some of the major enhancements are listed in the “enhancements” section of this document.

1. Installation
---------------

To compile the [MySQL][1] foreign data wrapper, MySQL's C client library is needed. This library can be downloaded from the official [MySQL website][1].

1. To build on POSIX-compliant systems you need to ensure the `pg_config` executable is in your path when you run `make`. This executable is typically in your PostgreSQL installation's `bin` directory. For example:

    ```
    $ export PATH=/usr/local/pgsql/bin/:$PATH
    ```

2. The `mysql_config` must also be in the path, it resides in the MySQL `bin` directory.

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

Not that we have tested the `mysql_fdw` extenion only on MacOS and Ubuntu systems, but other \*NIX's should also work.

Enhancements
------------

The following enhancements are added to the latest version of `mysql_fdw`:

### Write-able FDW
The previous version was only read-only, the latest version provides the write capability. The user can now issue insert/update and delete statements for the foreign tables using the mysql FDW. It uses the PG type casting mechanism to provide opposite type casting between mysql and PG data types.

### Connection Pooling
The latest version comes with a connection pooler that utilises the same mysql database connection for all the queries in the same session. The previous version would open a new mysql database connection for every query. This is a performance enhancement.

### Where clause push-down
The latest version will push-down the foreign table where clause to the foreign server. The where condition on the foreign table will be executed on the foreign server hence there will be fewer rows to to bring across to PostgreSQL. This is a performance feature.

### Column push-down
The previous version was fetching all the columns from the target foreign table. The latest version does the column push-down and only brings back the columns that are part of the select target list. This is a performance feature.

### Prepared Statment
(Refactoring for `select` queries to use prepared statement)

The `select` queries are now using prepared statements instead of simple query protocol.

Usage
-----

The following parameters can be set on a MySQL foreign server object:

  * `host`: Address or hostname of the MySQL server. Defaults to `127.0.0.1`
  * `port`: Port number of the MySQL server. Defaults to `3306`

The following parameters can be set on a MySQL foreign table object:

  * `dbname`: Name of the MySQL database to query. This is a mandatory option.
  * `table_name`: Name of the MySQL table, default is the same as foreign table.

The following parameters need to supplied while creating user mapping.

  * `username`: Username to use when connecting to MySQL.
  * `password`: Password to authenticate to the MySQL server with.


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

    CREATE FOREIGN TABLE warehouse(
         warehouse_id int,
         warehouse_name text,
         warehouse_created datetime)
    SERVER mysql_server
         OPTIONS (dbname 'db', table_name 'warehouse');


-- insert new rows in table

    INSERT INTO warehouse values (1, 'UPS', sysdate());
    INSERT INTO warehouse values (2, 'TV', sysdate());
    INSERT INTO warehouse values (3, 'Table', sysdate());


-- select from table

    SELECT * FROM warehouse;

    warehouse_id | warehouse_name | warehouse_created  

    --------------+----------------+--------------------

            1 | UPS            | 29-SEP-14 23:33:46

            2 | TV             | 29-SEP-14 23:34:25

            3 | Table          | 29-SEP-14 23:33:49


-- delete row from table

    DELETE FROM warehouse where warehouse_id = 3;


-- update a row of table

    UPDATE warehouse set warehouse_name = 'UPS_NEW' where warehouse_id = 1;


-- explain a table

    EXPLAIN SELECT warehouse_id, warehouse_name FROM warehouse WHERE warehouse_name LIKE 'TV' limit 1;

                                       QUERY PLAN                                                   
    Limit  (cost=10.00..11.00 rows=1 width=36)
    ->  Foreign Scan on warehouse  (cost=10.00..13.00 rows=3 width=36)
         Local server startup cost: 10
         Remote query: SELECT warehouse_id, warehouse_name FROM db.warehouse WHERE ((warehouse_name like 'TV'))
 Planning time: 0.564 ms
(5 rows)


Contributing
------------
If you experince any bug and have a fix for that, or have a new idea, send the detail along with the patch directly to mysql_fdw @ enterprisedb.com.
Before submitting a bugfix or new feature, please read the [contributing guidlines][4].

Support
-------

This project will be modified to maintain compatibility with new PostgreSQL releases. 

As with many open source projects, you may be able to obtain support via the public mailing list (mysql_fdw @ enterprisedb.com).
If you require commercial support, please contact the EnterpriseDB sales team, or check whether your existing PostgreSQL support provider can also support mysql_fdw.


License
-------
Copyright (c) 2011 - 2014, EnterpriseDB Corporation

Permission to use, copy, modify, and distribute this software and its
documentation for any purpose, without fee, and without a written agreement is
hereby granted, provided that the above copyright notice and this paragraph and
the following two paragraphs appear in all copies.

See the [`LICENSE`][5] file for full details.

[1]: http://www.mysql.com
[3]: https://github.com/EnterpriseDB/mysql_fdw/issues/new
[4]: CONTRIBUTING.md
[5]: LICENSE
