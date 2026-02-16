-- bug: COUNT(DISTINCT val) counts all rows instead of distinct values
-- setup:
CREATE TABLE t_cwd (grp TEXT, val INT);
INSERT INTO t_cwd VALUES ('a', 1), ('a', 1), ('a', 2), ('b', 3), ('b', 3);
-- input:
SELECT grp, COUNT(DISTINCT val) as uniq FROM t_cwd GROUP BY grp ORDER BY grp;
-- expected output:
a|2
b|1
