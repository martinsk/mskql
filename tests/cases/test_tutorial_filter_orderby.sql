-- tutorial: SELECT with WHERE and ORDER BY (index.html step 2)
-- setup:
CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE employees (id SERIAL PRIMARY KEY, name TEXT NOT NULL, dept_id INT REFERENCES departments(id), salary INT DEFAULT 0, hired DATE);
INSERT INTO departments (name) VALUES ('Engineering'), ('Design'), ('Sales');
INSERT INTO employees (name, dept_id, salary, hired) VALUES ('Alice', 1, 130000, '2022-03-15'), ('Bob', 1, 95000, '2023-01-10'), ('Carol', 2, 110000, '2021-07-01'), ('Dave', 2, 90000, '2024-02-20'), ('Eve', 3, 105000, '2023-06-01'), ('Frank', NULL, 80000, '2024-11-01');
-- input:
SELECT name, salary FROM employees WHERE salary > 100000 ORDER BY salary DESC;
-- expected output:
Alice|130000
Carol|110000
Eve|105000
-- expected status: 0
