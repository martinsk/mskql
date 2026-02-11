-- WHERE clause with modulo expression
-- setup:
CREATE TABLE t1 (val INT);
INSERT INTO t1 (val) VALUES (1), (2), (3), (4), (5), (6);
-- input:
SELECT val FROM t1 WHERE val % 2 = 0 ORDER BY val;
-- expected output:
2
4
6
-- expected status: 0
