-- CTE used in a JOIN
-- setup:
CREATE TABLE employees (id INT, name TEXT, dept_id INT);
INSERT INTO employees (id, name, dept_id) VALUES (1, 'alice', 10), (2, 'bob', 20), (3, 'carol', 10);
CREATE TABLE departments (id INT, dept_name TEXT);
INSERT INTO departments (id, dept_name) VALUES (10, 'eng'), (20, 'sales');
-- input:
WITH eng_staff AS (SELECT id, name, dept_id FROM employees WHERE dept_id = 10) SELECT eng_staff.name, departments.dept_name FROM eng_staff JOIN departments ON eng_staff.dept_id = departments.id ORDER BY eng_staff.name;
-- expected output:
alice|eng
carol|eng
-- expected status: 0
