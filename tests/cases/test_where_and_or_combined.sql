-- WHERE with AND and OR combined: (a = 1 AND b = 2) OR (a = 3)
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 1, 2), (2, 1, 3), (3, 3, 9), (4, 5, 5);
-- input:
SELECT id FROM t1 WHERE (a = 1 AND b = 2) OR a = 3 ORDER BY id;
-- expected output:
1
3
-- expected status: 0
