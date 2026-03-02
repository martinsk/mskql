-- Bug: GROUP BY ... HAVING SUM(col) > N returns empty result
-- SELECT g FROM t GROUP BY g HAVING SUM(v) > 50 should return qualifying groups
-- The plan executor intercepts this query but returns empty rows instead of applying the filter
-- setup:
CREATE TABLE t_hsum (g INT, v INT);
INSERT INTO t_hsum VALUES (1,100),(1,200),(2,5);
-- input:
SELECT g FROM t_hsum GROUP BY g HAVING SUM(v) > 50 ORDER BY g;
-- expected output:
1
-- expected status: 0
