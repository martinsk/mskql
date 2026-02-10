-- window AVG should skip NULLs in computation
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, val INT);
INSERT INTO t1 (id, dept, val) VALUES (1, 'a', 10), (2, 'a', NULL), (3, 'a', 30);
-- input:
SELECT id, dept, AVG(val) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|a|20
2|a|20
3|a|20
-- expected status: 0
