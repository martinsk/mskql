-- WHERE with AND/OR precedence: AND binds tighter than OR
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 1, 1), (2, 1, 0), (3, 0, 1), (4, 0, 0);
-- input:
SELECT id FROM t1 WHERE a = 1 OR b = 1 AND a = 0 ORDER BY id;
-- expected output:
1
2
3
-- expected status: 0
