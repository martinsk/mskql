-- NULLIF should return NULL when both args are equal
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 0), (2, 5), (3, 0);
-- input:
SELECT id, NULLIF(val, 0) FROM t1 ORDER BY id;
-- expected output:
1|
2|5
3|
-- expected status: 0
