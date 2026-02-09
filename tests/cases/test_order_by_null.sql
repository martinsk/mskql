-- ORDER BY with NULL values: NULLs should sort consistently (typically last in ASC)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20);
-- input:
SELECT id, val FROM t1 ORDER BY val;
-- expected output:
3|10
5|20
1|30
2|
4|
-- expected status: 0
