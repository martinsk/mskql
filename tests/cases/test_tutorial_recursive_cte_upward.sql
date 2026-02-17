-- tutorial: recursive CTEs - walk upward to root (recursive-ctes.html)
-- setup:
CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, manager_id INT REFERENCES employees(id), salary INT NOT NULL);
INSERT INTO employees (name, title, manager_id, salary) VALUES ('Alice', 'CEO', NULL, 200000), ('Bob', 'VP Engineering', 1, 160000), ('Charlie', 'VP Sales', 1, 155000), ('Diana', 'Tech Lead', 2, 130000), ('Eve', 'Senior Engineer', 2, 120000), ('Frank', 'Sales Manager', 3, 110000), ('Grace', 'Engineer', 4, 100000), ('Hank', 'Engineer', 4, 95000), ('Ivy', 'Sales Rep', 6, 80000), ('Jack', 'Sales Rep', 6, 75000);
-- input:
WITH RECURSIVE chain AS (SELECT id, name, title, manager_id, 0 AS depth FROM employees WHERE name = 'Grace' UNION ALL SELECT e.id, e.name, e.title, e.manager_id, c.depth + 1 FROM employees e JOIN chain c ON e.id = c.manager_id) SELECT name, title, depth FROM chain ORDER BY depth;
-- expected output:
Grace|Engineer|0
Diana|Tech Lead|1
Bob|VP Engineering|2
Alice|CEO|3
-- expected status: 0
