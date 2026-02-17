-- IS NOT FALSE should include NULL rows (NULL is not false)
-- setup:
CREATE TABLE t (id INT, flag BOOLEAN);
INSERT INTO t VALUES (1, true), (2, false), (3, NULL);
-- input:
SELECT * FROM t WHERE flag IS NOT FALSE ORDER BY id;
-- expected output:
1|t
3|
-- expected status: 0
