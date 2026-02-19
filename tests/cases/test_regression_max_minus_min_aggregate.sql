-- bug: arithmetic between multiple aggregates fails (e.g. MAX(val) - MIN(val))
-- setup:
CREATE TABLE t_agg_range (id INT, val INT);
INSERT INTO t_agg_range VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT MAX(val) - MIN(val) FROM t_agg_range;
-- expected output:
20
-- expected status: 0
