-- subquery in WHERE IN clause
-- setup:
CREATE TABLE "departments" (id INT, name TEXT);
INSERT INTO "departments" (id, name) VALUES (1, 'engineering'), (2, 'sales');
CREATE TABLE "employees" (id INT, name TEXT, dept_id INT);
INSERT INTO "employees" (id, name, dept_id) VALUES (1, 'alice', 1), (2, 'bob', 2), (3, 'charlie', 1), (4, 'dave', 3);
-- input:
SELECT name FROM employees WHERE dept_id IN (SELECT id FROM departments) ORDER BY name;
-- expected output:
alice
bob
charlie
-- expected status: 0
