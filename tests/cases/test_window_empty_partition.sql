-- window function ROW_NUMBER with partition that has single row per group
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, salary INT);
INSERT INTO t1 (id, dept, salary) VALUES (1, 'eng', 100), (2, 'sales', 200), (3, 'hr', 150);
-- input:
SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) FROM t1 ORDER BY id;
-- expected output:
1|eng|1
2|sales|1
3|hr|1
-- expected status: 0
