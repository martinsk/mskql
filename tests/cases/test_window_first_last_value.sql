-- FIRST_VALUE and LAST_VALUE window functions
-- setup:
CREATE TABLE t1 (id INT, grp TEXT, val INT);
INSERT INTO t1 VALUES (1, 'a', 10), (2, 'a', 20), (3, 'b', 30), (4, 'b', 40);
-- input:
SELECT id, grp, FIRST_VALUE(val) OVER (PARTITION BY grp ORDER BY id) AS first_v FROM t1 ORDER BY id;
-- expected output:
1|a|10
2|a|10
3|b|30
4|b|30
-- expected status: 0
