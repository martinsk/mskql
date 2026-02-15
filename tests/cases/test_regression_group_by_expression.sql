-- regression: GROUP BY expression groups correctly
-- setup:
CREATE TABLE t (val INT);
INSERT INTO t VALUES (1),(2),(3),(11),(12),(21);
-- input:
SELECT val % 10 as r, COUNT(*) FROM t GROUP BY val % 10 ORDER BY r;
-- expected output:
1|3
2|2
3|1
