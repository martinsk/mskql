-- BUG: NTH_VALUE with default frame should return NULL when nth row not yet in frame
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val, NTH_VALUE(val, 2) OVER (ORDER BY val) FROM t ORDER BY val;
-- expected output:
1|10|
2|20|20
3|30|20
-- expected status: 0
