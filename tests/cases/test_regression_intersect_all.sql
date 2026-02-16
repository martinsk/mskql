-- bug: INTERSECT ALL returns too many copies instead of min(count) per value
-- setup:
CREATE TABLE t_ia1 (val INT);
CREATE TABLE t_ia2 (val INT);
INSERT INTO t_ia1 VALUES (1), (1), (2), (3);
INSERT INTO t_ia2 VALUES (1), (3);
-- input:
SELECT val FROM t_ia1 INTERSECT ALL SELECT val FROM t_ia2 ORDER BY val;
-- expected output:
1
3
