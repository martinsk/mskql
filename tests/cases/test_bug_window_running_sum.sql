-- bug: SUM() OVER (ORDER BY ...) returns partition total instead of cumulative running sum
-- setup:
CREATE TABLE t_wsum (id INT, grp TEXT, val INT);
INSERT INTO t_wsum VALUES (1,'a',10),(2,'a',20),(3,'a',30),(4,'b',100),(5,'b',200);
-- input:
SELECT id, grp, val, SUM(val) OVER (PARTITION BY grp ORDER BY id) as running FROM t_wsum ORDER BY id;
-- expected output:
1|a|10|10
2|a|20|30
3|a|30|60
4|b|100|100
5|b|200|300
