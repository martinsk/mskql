-- window COUNT should count all rows in partition (including NULLs in other cols)
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, val INT);
INSERT INTO t1 (id, dept, val) VALUES (1, 'a', 10), (2, 'a', NULL), (3, 'b', 30);
-- input:
SELECT id, dept, COUNT(*) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|a|2
2|a|2
3|b|1
-- expected status: 0
