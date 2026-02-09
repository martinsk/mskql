-- LATERAL join
-- setup:
CREATE TABLE departments (id INT, name TEXT);
INSERT INTO departments (id, name) VALUES (1, 'eng'), (2, 'sales');
CREATE TABLE staff (dept_id INT, emp_name TEXT, salary INT);
INSERT INTO staff (dept_id, emp_name, salary) VALUES (1, 'alice', 100), (1, 'bob', 150), (2, 'carol', 120), (2, 'dave', 80);
-- input:
SELECT departments.name, top.emp_name, top.salary FROM departments JOIN LATERAL (SELECT emp_name, salary FROM staff WHERE staff.dept_id = departments.id ORDER BY salary DESC LIMIT 1) AS top ON TRUE ORDER BY departments.name;
-- expected output:
eng|bob|150
sales|carol|120
-- expected status: 0
