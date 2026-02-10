-- NULLIF with NULL first arg should return NULL
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL), (2, 5);
-- input:
SELECT id, NULLIF(val, 0) FROM t1 ORDER BY id;
-- expected output:
1|
2|5
-- expected status: 0
