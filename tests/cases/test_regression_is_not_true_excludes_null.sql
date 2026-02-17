-- IS NOT TRUE should include NULL rows (NULL is not true)
-- setup:
CREATE TABLE t (id INT, flag BOOLEAN);
INSERT INTO t VALUES (1, true), (2, false), (3, NULL);
-- input:
SELECT * FROM t WHERE flag IS NOT TRUE ORDER BY id;
-- expected output:
2|f
3|
-- expected status: 0
