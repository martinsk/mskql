-- multiple CTEs
-- setup:
CREATE TABLE employees (id INT, name TEXT, dept TEXT);
INSERT INTO employees (id, name, dept) VALUES (1, 'alice', 'eng'), (2, 'bob', 'eng'), (3, 'carol', 'sales');
CREATE TABLE salaries (emp_id INT, amount INT);
INSERT INTO salaries (emp_id, amount) VALUES (1, 100), (2, 150), (3, 120);
-- input:
WITH eng AS (SELECT id, name FROM employees WHERE dept = 'eng'), eng_sal AS (SELECT eng.name, salaries.amount FROM eng JOIN salaries ON eng.id = salaries.emp_id) SELECT name, amount FROM eng_sal ORDER BY name;
-- expected output:
alice|100
bob|150
-- expected status: 0
