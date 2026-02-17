-- ORDER BY SUM(val) DESC should sort by the aggregate value
-- setup:
CREATE TABLE t (cat TEXT, val INT);
INSERT INTO t VALUES ('a',10),('a',20),('b',30),('b',5);
-- input:
SELECT cat, SUM(val) FROM t GROUP BY cat ORDER BY SUM(val) DESC;
-- expected output:
b|35
a|30
-- expected status: 0
