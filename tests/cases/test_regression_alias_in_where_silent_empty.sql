-- bug: using SELECT alias in WHERE silently returns empty instead of raising an error
-- setup:
CREATE TABLE t_alias_where (id INT, val INT);
INSERT INTO t_alias_where VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id AS i, val AS v FROM t_alias_where WHERE v > 15;
-- expected output:
ERROR:  column "v" does not exist
-- expected status: 0
