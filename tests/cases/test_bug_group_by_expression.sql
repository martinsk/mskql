-- bug: GROUP BY expression (val % 10) treats each row as unique instead of grouping
-- setup:
CREATE TABLE t_gbe (val INT);
INSERT INTO t_gbe VALUES (1), (2), (3), (11), (12), (21);
-- input:
SELECT val % 10 as remainder, COUNT(*) as cnt FROM t_gbe GROUP BY val % 10 ORDER BY remainder;
-- expected output:
1|3
2|2
3|1
