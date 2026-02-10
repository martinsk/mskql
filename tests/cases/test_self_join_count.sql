-- Self-join to find pairs: count employees in same department
-- setup:
CREATE TABLE emp (id INT, name TEXT, dept TEXT);
INSERT INTO emp (id, name, dept) VALUES (1, 'alice', 'eng'), (2, 'bob', 'eng'), (3, 'carol', 'sales');
-- input:
SELECT a.name, b.name FROM emp a JOIN emp b ON a.dept = b.dept AND a.id < b.id ORDER BY a.name;
-- expected output:
alice|bob
-- expected status: 0
