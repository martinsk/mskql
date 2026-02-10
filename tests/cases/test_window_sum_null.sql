-- window SUM should skip NULL values in accumulation
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, val INT);
INSERT INTO t1 (id, dept, val) VALUES (1, 'a', 10), (2, 'a', NULL), (3, 'a', 30), (4, 'b', 20);
-- input:
SELECT id, dept, SUM(val) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|a|40
2|a|40
3|a|40
4|b|20
-- expected status: 0
