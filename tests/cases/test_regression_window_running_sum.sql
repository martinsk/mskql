-- regression: SUM OVER ORDER BY produces cumulative running sum
-- setup:
CREATE TABLE t (id INT, grp TEXT, val INT);
INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'a',30),(4,'b',100),(5,'b',200);
-- input:
SELECT id, grp, SUM(val) OVER (PARTITION BY grp ORDER BY id) FROM t ORDER BY id;
-- expected output:
1|a|10
2|a|30
3|a|60
4|b|100
5|b|300
