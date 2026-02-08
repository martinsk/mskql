-- CTE with clause
-- setup:
CREATE TABLE employees (id INT, name TEXT, dept TEXT);
INSERT INTO employees (id, name, dept) VALUES (1, 'alice', 'eng'), (2, 'bob', 'eng'), (3, 'charlie', 'sales');
-- input:
WITH eng AS (SELECT id, name FROM employees WHERE dept = 'eng') SELECT id, name FROM eng ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
