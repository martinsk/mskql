-- playground example: joins (INNER JOIN with ORDER BY)
-- setup:
CREATE TABLE departments (id INT PRIMARY KEY, name TEXT);
INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Marketing'), (3, 'Sales');
CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT);
INSERT INTO employees VALUES (1, 'Alice', 1, 95000), (2, 'Bob', 1, 88000), (3, 'Charlie', 2, 72000), (4, 'Diana', 3, 81000), (5, 'Eve', 2, 69000);
-- input:
SELECT e.name, d.name AS department, e.salary FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.salary DESC;
-- expected output:
Alice|Engineering|95000
Bob|Engineering|88000
Diana|Sales|81000
Charlie|Marketing|72000
Eve|Marketing|69000
-- expected status: 0
