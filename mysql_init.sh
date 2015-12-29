#!/bin/sh
export MYSQL_PWD="bar"
mysql -h 127.0.0.1 -u foo -D testdb -e "DROP DATABASE IF EXISTS testdb"
mysql -h 127.0.0.1 -u foo -e "CREATE DATABASE testdb"
mysql -h 127.0.0.1 -u foo -D testdb -e "CREATE TABLE department(department_id int, department_name text, PRIMARY KEY (department_id))"
mysql -h 127.0.0.1 -u foo -D testdb -e "CREATE TABLE employee(emp_id int, emp_name text, emp_dept_id int, PRIMARY KEY (emp_id))"
mysql -h 127.0.0.1 -u foo -D testdb -e "CREATE TABLE empdata (emp_id int, emp_dat blob, PRIMARY KEY (emp_id))"

