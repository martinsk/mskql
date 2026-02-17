-- tutorial: recursive CTEs - full org chart walk (recursive-ctes.html)
-- setup:
CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, manager_id INT REFERENCES employees(id), salary INT NOT NULL);
INSERT INTO employees (name, title, manager_id, salary) VALUES ('Alice', 'CEO', NULL, 200000), ('Bob', 'VP Engineering', 1, 160000), ('Charlie', 'VP Sales', 1, 155000), ('Diana', 'Tech Lead', 2, 130000), ('Eve', 'Senior Engineer', 2, 120000), ('Frank', 'Sales Manager', 3, 110000), ('Grace', 'Engineer', 4, 100000), ('Hank', 'Engineer', 4, 95000), ('Ivy', 'Sales Rep', 6, 80000), ('Jack', 'Sales Rep', 6, 75000);
-- input:
WITH RECURSIVE org AS (SELECT id, name, title, manager_id, 0 AS depth FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.title, e.manager_id, o.depth + 1 FROM employees e JOIN org o ON e.manager_id = o.id) SELECT name, title, depth FROM org ORDER BY depth, name;
-- expected output:
Alice|CEO|0
Bob|VP Engineering|1
Charlie|VP Sales|1
Diana|Tech Lead|2
Eve|Senior Engineer|2
Frank|Sales Manager|2
Grace|Engineer|3
Hank|Engineer|3
Ivy|Sales Rep|3
Jack|Sales Rep|3
-- expected status: 0
