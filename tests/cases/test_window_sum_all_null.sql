-- window SUM where all partition values are NULL should return 0 or NULL
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, val INT);
INSERT INTO t1 (id, dept, val) VALUES (1, 'a', NULL), (2, 'a', NULL), (3, 'b', 10);
-- input:
SELECT id, dept, SUM(val) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|a|0
2|a|0
3|b|10
-- expected status: 0
