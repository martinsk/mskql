-- bug: STRING_AGG with ORDER BY clause fails to parse ('expected ) after aggregate expression')
-- setup:
CREATE TABLE t_sa_order (id INT, grp TEXT, val INT);
INSERT INTO t_sa_order VALUES (1,'a',30),(2,'a',10),(3,'a',20),(4,'b',50),(5,'b',40);
-- input:
SELECT grp, STRING_AGG(val::TEXT, ',' ORDER BY val) FROM t_sa_order GROUP BY grp ORDER BY grp;
-- expected output:
a|10,20,30
b|40,50
-- expected status: 0
