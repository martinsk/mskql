-- tutorial: recursive CTEs - subtree under Bob (recursive-ctes.html)
-- setup:
CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, manager_id INT REFERENCES employees(id), salary INT NOT NULL);
INSERT INTO employees (name, title, manager_id, salary) VALUES ('Alice', 'CEO', NULL, 200000), ('Bob', 'VP Engineering', 1, 160000), ('Charlie', 'VP Sales', 1, 155000), ('Diana', 'Tech Lead', 2, 130000), ('Eve', 'Senior Engineer', 2, 120000), ('Frank', 'Sales Manager', 3, 110000), ('Grace', 'Engineer', 4, 100000), ('Hank', 'Engineer', 4, 95000), ('Ivy', 'Sales Rep', 6, 80000), ('Jack', 'Sales Rep', 6, 75000);
-- input:
WITH RECURSIVE team AS (SELECT id, name, title, 0 AS depth FROM employees WHERE name = 'Bob' UNION ALL SELECT e.id, e.name, e.title, t.depth + 1 FROM employees e JOIN team t ON e.manager_id = t.id) SELECT name, title, depth FROM team ORDER BY depth, name;
-- expected output:
Bob|VP Engineering|0
Diana|Tech Lead|1
Eve|Senior Engineer|1
Grace|Engineer|2
Hank|Engineer|2
-- expected status: 0
