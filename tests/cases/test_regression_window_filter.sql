-- BUG: FILTER clause on window function not supported
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT id, val, SUM(val) FILTER (WHERE val > 20) OVER (ORDER BY id) FROM t ORDER BY id;
-- expected output:
1|10|
2|20|
3|30|30
4|40|70
5|50|120
-- expected status: 0
