-- tutorial: INNER JOIN (index.html step 3)
-- setup:
CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, dept_id INT REFERENCES departments(id), salary INT DEFAULT 0, hired DATE);
INSERT INTO departments (name) VALUES ('Engineering'), ('Design'), ('Sales');
INSERT INTO employees (name, dept_id, salary, hired) VALUES ('Alice', 1, 130000, '2022-03-15'), ('Bob', 1, 95000, '2023-01-10'), ('Carol', 2, 110000, '2021-07-01'), ('Dave', 2, 90000, '2024-02-20'), ('Eve', 3, 105000, '2023-06-01'), ('Frank', NULL, 80000, '2024-11-01');
-- input:
SELECT e.name, d.name AS department FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.name;
-- expected output:
Alice|Engineering
Bob|Engineering
Carol|Design
Dave|Design
Eve|Sales
-- expected status: 0
