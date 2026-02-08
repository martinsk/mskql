-- natural join
-- setup:
CREATE TABLE dept (id INT, dname TEXT);
INSERT INTO dept (id, dname) VALUES (1, 'eng'), (2, 'sales');
CREATE TABLE emp (id INT, ename TEXT, dept_id INT);
INSERT INTO emp (id, ename, dept_id) VALUES (10, 'alice', 1), (20, 'bob', 2);
-- input:
SELECT emp.ename, dept.dname FROM emp JOIN dept ON emp.dept_id = dept.id ORDER BY emp.ename;
-- expected output:
alice|eng
bob|sales
-- expected status: 0
