CREATE EXTENSION mysql_fdw;
CREATE SERVER mysql_svr FOREIGN DATA WRAPPER mysql_fdw;
CREATE USER MAPPING FOR postgres SERVER mysql_svr OPTIONS(username 'foo', password 'bar');

CREATE FOREIGN TABLE department(department_id int, department_name text) SERVER mysql_svr OPTIONS(dbname 'testdb', table_name 'department');
CREATE FOREIGN TABLE employee(emp_id int, emp_name text, emp_dept_id int) SERVER mysql_svr OPTIONS(dbname 'testdb', table_name 'employee');

SELECT * FROM department LIMIT 10;
SELECT * FROM employee LIMIT 10;

INSERT INTO department VALUES(generate_series(1,1000), 'dept - ' || generate_series(1,1000));
INSERT INTO employee VALUES(generate_series(1,10000), 'emp - ' || generate_series(1,10000), generate_series(1,1000));

SELECT count(*) FROM department;
SELECT count(*) FROM employee;

EXPLAIN (COSTS FALSE) SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;

EXPLAIN (COSTS FALSE) SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;

SELECT * FROM department d, employee e WHERE d.department_id = e.emp_dept_id LIMIT 10;
SELECT * FROM department d, employee e WHERE d.department_id IN (SELECT department_id FROM department) LIMIT 10;

DELETE FROM employee WHERE emp_id = 1000;

SELECT COUNT(*) FROM department LIMIT 10;
SELECT COUNT(*) FROM employee WHERE emp_id = 1000;

UPDATE employee SET emp_name = 'Updated emp' WHERE emp_id = 2000;
SELECT emp_id, emp_name FROM employee WHERE emp_name like 'Updated emp';


DELETE FROM employee;
DELETE FROM department;

DROP FOREIGN TABLE department;
DROP FOREIGN TABLE employee;
DROP USER MAPPING FOR postgres SERVER mysql_svr;
DROP SERVER mysql_svr;
DROP EXTENSION mysql_fdw;
