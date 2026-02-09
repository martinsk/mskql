-- window AVG over partition
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, salary INT);
INSERT INTO t1 (id, dept, salary) VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150);
-- input:
SELECT id, dept, AVG(salary) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|eng|150
2|eng|150
3|sales|150
-- expected status: 0
