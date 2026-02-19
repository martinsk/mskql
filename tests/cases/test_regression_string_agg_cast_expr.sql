-- bug: STRING_AGG with cast expression argument returns empty instead of concatenated values
-- setup:
CREATE TABLE t_sa_cast (id INT, grp TEXT, val INT);
INSERT INTO t_sa_cast VALUES (1,'a',10),(2,'a',20),(3,'b',30);
-- input:
SELECT grp, STRING_AGG(val::TEXT, ',') FROM t_sa_cast GROUP BY grp ORDER BY grp;
-- expected output:
a|10,20
b|30
-- expected status: 0
