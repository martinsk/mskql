-- BUG: Window RANGE frame with value offset treats it as UNBOUNDED, returning total sum for all rows
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 20), (4, 30), (5, 40);
-- input:
SELECT id, val, SUM(val) OVER (ORDER BY val RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING) FROM t ORDER BY id;
-- expected output:
1|10|50
2|20|80
3|20|80
4|30|110
5|40|70
-- expected status: 0
