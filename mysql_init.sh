#!/bin/sh
export MYSQL_PWD="edb"
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER_NAME="edb"

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

mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE mysql_test(a int primary key, b int);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO mysql_test(a,b) VALUES (1,1);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE empdata (emp_id int, emp_dat blob, PRIMARY KEY (emp_id));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE numbers (a int PRIMARY KEY, b varchar(255));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test_tbl1 (c1 INT primary key, c2 VARCHAR(10), c3 CHAR(9), c4 MEDIUMINT, c5 DATE, c6 DECIMAL(10,5), c7 INT, c8 SMALLINT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE test_tbl2 (c1 INT primary key, c2 TEXT, c3 TEXT);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "CREATE TABLE student (stu_id int PRIMARY KEY, stu_name text);"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress1 -e "CREATE TABLE numbers (a int, b varchar(255));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "CREATE TABLE enum_t1 (id int PRIMARY KEY, size ENUM('small', 'medium', 'large'));"
mysql -h $MYSQL_HOST -u $MYSQL_USER_NAME -P $MYSQL_PORT -D mysql_fdw_regress -e "INSERT INTO enum_t1 VALUES (1, 'small'),(2, 'medium'),(3, 'medium');"
