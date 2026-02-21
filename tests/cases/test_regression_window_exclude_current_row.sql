-- BUG: EXCLUDE CURRENT ROW in window frame not supported
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT id, val, SUM(val) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE CURRENT ROW) FROM t ORDER BY id;
-- expected output:
1|10|20
2|20|40
3|30|60
4|40|80
5|50|40
-- expected status: 0
