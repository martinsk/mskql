-- bug: COALESCE wrapping aggregates on empty table returns no rows instead of fallback value
-- setup:
CREATE TABLE t_coalesce_empty (id INT);
-- input:
SELECT COALESCE(MAX(id), 0) FROM t_coalesce_empty;
-- expected output:
0
-- expected status: 0
