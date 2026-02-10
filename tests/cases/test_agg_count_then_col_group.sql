-- SELECT COUNT(*), col FROM t GROUP BY col — aggregate listed before plain column
-- Parser expects only aggregates after first aggregate in parse_agg_list
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT);
INSERT INTO t1 (dept, name) VALUES ('eng', 'alice'), ('eng', 'bob'), ('sales', 'carol');
-- input:
SELECT COUNT(*), dept FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
2|eng
1|sales
-- expected status: 0
