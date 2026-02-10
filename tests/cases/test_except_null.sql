-- EXCEPT should handle NULL rows correctly (NULL = NULL for set ops)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, NULL), (3, 30);
CREATE TABLE t2 (id INT, val INT);
INSERT INTO t2 (id, val) VALUES (1, 10);
-- input:
SELECT id, val FROM t1 EXCEPT SELECT id, val FROM t2 ORDER BY id;
-- expected output:
2|
3|30
-- expected status: 0
