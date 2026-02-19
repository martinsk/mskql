-- bug: mixing window function with other columns and a cast in arithmetic expression fails to parse
-- setup:
CREATE TABLE t_win_expr (id INT, grp TEXT, val INT);
INSERT INTO t_win_expr VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40);
-- input:
SELECT id, grp, val, SUM(val) OVER (PARTITION BY grp) AS grp_total, val::FLOAT / SUM(val) OVER (PARTITION BY grp) AS pct FROM t_win_expr ORDER BY id;
-- expected output:
1|a|10|30|0.333333
2|a|20|30|0.666667
3|b|30|70|0.428571
4|b|40|70|0.571429
-- expected status: 0
