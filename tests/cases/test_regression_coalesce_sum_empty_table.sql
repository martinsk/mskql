-- bug: COALESCE(SUM(...), 0) on empty table returns no rows instead of 0
-- setup:
CREATE TABLE t_coalesce_sum_empty (id INT, val INT);
-- input:
SELECT COALESCE(SUM(val), 0) FROM t_coalesce_sum_empty;
-- expected output:
0
-- expected status: 0
