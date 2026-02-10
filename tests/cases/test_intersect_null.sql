-- INTERSECT should treat NULL = NULL for set operations
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL);
CREATE TABLE t2 (id INT, val INT);
INSERT INTO t2 (id, val) VALUES (1, 10), (2, NULL);
-- input:
SELECT id, val FROM t1 INTERSECT SELECT id, val FROM t2 ORDER BY id;
-- expected output:
1|10
2|
-- expected status: 0
