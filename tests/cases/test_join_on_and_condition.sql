-- JOIN with compound ON condition using AND
-- setup:
CREATE TABLE t1 (id INT, dept TEXT);
INSERT INTO t1 (id, dept) VALUES (1, 'eng'), (2, 'sales');
CREATE TABLE t2 (emp_id INT, dept TEXT, score INT);
INSERT INTO t2 (emp_id, dept, score) VALUES (1, 'eng', 90), (1, 'sales', 50), (2, 'sales', 80);
-- input:
SELECT t1.id, t2.score FROM t1 JOIN t2 ON t1.id = t2.emp_id AND t1.dept = t2.dept ORDER BY t1.id;
-- expected output:
1|90
2|80
-- expected status: 0
