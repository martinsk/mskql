-- adversarial: GROUP BY on empty table
-- setup:
CREATE TABLE t_gbe (category TEXT, value INT);
-- input:
SELECT category, SUM(value) FROM t_gbe GROUP BY category;
-- expected output:
