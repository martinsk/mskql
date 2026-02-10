-- WHERE with AND/OR precedence: AND binds tighter than OR
-- a = 1 OR b = 2 AND c = 3 should be parsed as a = 1 OR (b = 2 AND c = 3)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT, c INT);
INSERT INTO t1 (id, a, b, c) VALUES (1, 1, 0, 0), (2, 0, 2, 3), (3, 0, 2, 0), (4, 0, 0, 3);
-- input:
SELECT id FROM t1 WHERE a = 1 OR b = 2 AND c = 3 ORDER BY id;
-- expected output:
1
2
-- expected status: 0
