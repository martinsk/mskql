-- bug: PARTITION BY with multiple columns fails with "unexpected token in OVER clause"
-- setup:
CREATE TABLE t_wpm (a TEXT, b TEXT, val INT);
INSERT INTO t_wpm VALUES ('x', 'p', 1), ('x', 'p', 2), ('x', 'q', 3), ('y', 'p', 4), ('y', 'q', 5);
-- input:
SELECT a, b, val, SUM(val) OVER (PARTITION BY a, b) as grp_sum FROM t_wpm ORDER BY a, b, val;
-- expected output:
x|p|1|3
x|p|2|3
x|q|3|3
y|p|4|4
y|q|5|5
