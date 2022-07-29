#!/bin/sh
export MYSQL_PWD="${MYSQL_PWD:=edb}"
export MYSQL_HOST="${MYSQL_HOST:=localhost}"
export MYSQL_PORT="${MYSQL_PORT:=3306}"
export MYSQL_USER_NAME="${MYSQL_USER_NAME:=edb}"

# Below commands must be run first time to create mysql_fdw_regress and mysql_fdw_regress1 databases
# used in regression tests with edb user and edb password.
# --connect to mysql with root user
# mysql -u root -p

# --run below
# CREATE DATABASE mysql_fdw_regress;
# CREATE DATABASE mysql_fdw_regress1;
# SET GLOBAL validate_password.policy = LOW;
# SET GLOBAL validate_password.length = 1;
# SET GLOBAL validate_password.mixed_case_count = 0;
# SET GLOBAL validate_password.number_count = 0;
# SET GLOBAL validate_password.special_char_count = 0;
# CREATE USER 'edb'@'localhost' IDENTIFIED BY 'edb';
# GRANT ALL PRIVILEGES ON mysql_fdw_regress.* TO 'edb'@'localhost';
# GRANT ALL PRIVILEGES ON mysql_fdw_regress1.* TO 'edb'@'localhost';

mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS mysql_test;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS empdata;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS numbers;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test_tbl2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test_tbl1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "DROP TABLE IF EXISTS student;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "DROP TABLE IF EXISTS numbers;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS enum_t1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "DROP TABLE IF EXISTS dept;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "DROP TABLE IF EXISTS student1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS enum_t2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test1;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test2;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test3;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test4;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test5;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test_set;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test6;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS test7;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS distinct_test;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS fdw520;"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "DROP TABLE IF EXISTS fdw520_1;"

mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE mysql_test(a int primary key, b int not null);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO mysql_test(a,b) VALUES (1,1);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE empdata (emp_id int, emp_dat blob, PRIMARY KEY (emp_id));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE numbers (a int PRIMARY KEY, b varchar(255));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test_tbl1 (c1 INT primary key, c2 VARCHAR(10), c3 CHAR(9), c4 MEDIUMINT, c5 DATE, c6 DECIMAL(10,5), c7 INT, c8 SMALLINT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test_tbl2 (c1 INT primary key, c2 TEXT, c3 TEXT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "CREATE TABLE student (stu_id int PRIMARY KEY, stu_name text, stu_dept int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "CREATE TABLE numbers (a int, b varchar(255));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE enum_t1 (id int PRIMARY KEY, size ENUM('small', 'medium', 'large'));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "CREATE TABLE student1 (stu_id varchar(10) PRIMARY KEY, stu_name text, stu_dept int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE enum_t2 (id int PRIMARY KEY, size ENUM('S', 'M', 'L'));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO enum_t2 VALUES (10, 'S'),(20, 'M'),(30, 'M');"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test1 (c1 int PRIMARY KEY, c2 int, c3 varchar(255), c4 ENUM ('foo', 'bar', 'buz'))"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test2 (c1 int PRIMARY KEY, c2 int, c3 varchar(255), c4 ENUM ('foo', 'bar', 'buz'))"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test3 (c1 int PRIMARY KEY, c2 int, c3 varchar(255))"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test4 (c1 int PRIMARY KEY, c2 int, c3 varchar(255))"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test5 (c1 int primary key, c2 binary, c3 binary(3), c4 binary(1), c5 binary(10), c6 varbinary(3), c7 varbinary(1), c8 varbinary(10), c9 binary(0), c10 varbinary(0));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO test5 VALUES (1, 'c', 'c3c', 't', 'c5c5c5', '04', '1', '01-10-2021', NULL, '');"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test_set (c1 int primary key, c2 SET('a', 'b', 'c', 'd'), c3 int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test6 (c1 numeric(6,4))"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO test6 VALUES (25.252525)"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test7 (c1 int primary key auto_increment, c2 longtext NOT NULL);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO test7 (c2) SELECT repeat('abcdefgh ', 7500);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE distinct_test (id int primary key, c1 int, c2 int, c3 text, c4 text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "CREATE TABLE dept (dept_id int PRIMARY KEY, stu_id varchar(10), FOREIGN KEY (stu_id) REFERENCES student1(stu_id));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e 'CREATE TABLE fdw520 (c1 int primary key, `c ``"" 2` int, c3 int generated always as (`c ``"" 2` * 2) stored, c4 int generated always as (`c ``"" 2` * 4) virtual not null);'
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE fdw520_1 (c1 int primary key, c2 int, c3 int generated always as (c2 * 2) stored, c4 int generated always as (c3 * 4) stored not null);"
