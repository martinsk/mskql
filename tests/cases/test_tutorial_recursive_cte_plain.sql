-- tutorial: recursive CTEs - plain (non-recursive) CTE (recursive-ctes.html)
-- setup:
CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, manager_id INT REFERENCES employees(id), salary INT NOT NULL);
INSERT INTO employees (name, title, manager_id, salary) VALUES ('Alice', 'CEO', NULL, 200000), ('Bob', 'VP Engineering', 1, 160000), ('Charlie', 'VP Sales', 1, 155000), ('Diana', 'Tech Lead', 2, 130000), ('Eve', 'Senior Engineer', 2, 120000), ('Frank', 'Sales Manager', 3, 110000), ('Grace', 'Engineer', 4, 100000), ('Hank', 'Engineer', 4, 95000), ('Ivy', 'Sales Rep', 6, 80000), ('Jack', 'Sales Rep', 6, 75000);
-- input:
WITH dept_salary AS (SELECT manager_id, COUNT(*) AS reports, AVG(salary) AS avg_salary FROM employees WHERE manager_id IS NOT NULL GROUP BY manager_id) SELECT e.name, e.title, d.reports, d.avg_salary FROM employees e JOIN dept_salary d ON e.id = d.manager_id ORDER BY d.reports DESC;
-- expected output:
Alice|CEO|2|157500
Bob|VP Engineering|2|125000
Diana|Tech Lead|2|97500
Frank|Sales Manager|2|77500
Charlie|VP Sales|1|110000
-- expected status: 0
