-- window function with NULL in partition column should group NULLs together
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, salary INT);
INSERT INTO t1 (id, dept, salary) VALUES (1, 'eng', 100), (2, NULL, 200), (3, 'eng', 150), (4, NULL, 50);
-- input:
SELECT id, dept, SUM(salary) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|eng|250
2||250
3|eng|250
4||250
-- expected status: 0
