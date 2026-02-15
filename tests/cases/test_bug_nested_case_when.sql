-- bug: nested CASE WHEN consumes outer WHEN clauses after the inner END
-- setup:
CREATE TABLE t_ncw (id INT, val INT);
INSERT INTO t_ncw VALUES (1, 5), (2, 15), (3, 25), (4, 35);
-- input:
SELECT id, CASE WHEN val < 10 THEN 'low' WHEN val < 20 THEN CASE WHEN val < 16 THEN 'mid-low' ELSE 'mid-high' END WHEN val < 30 THEN 'high' ELSE 'very high' END as label FROM t_ncw ORDER BY id;
-- expected output:
1|low
2|mid-low
3|high
4|very high
