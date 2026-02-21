-- BUG: GROUPS frame type in window functions not supported
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 10), (3, 20), (4, 30), (5, 30);
-- input:
SELECT id, val, SUM(val) OVER (ORDER BY val GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t ORDER BY id;
-- expected output:
1|10|40
2|10|40
3|20|100
4|30|80
5|30|80
-- expected status: 0
