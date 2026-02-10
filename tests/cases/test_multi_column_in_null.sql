-- multi-column IN with NULL should not match (SQL three-valued logic)
-- setup:
CREATE TABLE t1 (a INT, b INT);
INSERT INTO t1 (a, b) VALUES (1, 2), (3, NULL), (5, 6);
-- input:
SELECT a, b FROM t1 WHERE (a, b) IN ((1, 2), (3, 4)) ORDER BY a;
-- expected output:
1|2
-- expected status: 0
