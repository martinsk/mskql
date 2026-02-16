-- bug: chaining three UNION ALL duplicates rows from the last operand
-- setup:
CREATE TABLE t_cu1 (val INT);
CREATE TABLE t_cu2 (val INT);
CREATE TABLE t_cu3 (val INT);
INSERT INTO t_cu1 VALUES (1);
INSERT INTO t_cu2 VALUES (2);
INSERT INTO t_cu3 VALUES (3);
-- input:
SELECT val FROM t_cu1 UNION ALL SELECT val FROM t_cu2 UNION ALL SELECT val FROM t_cu3 ORDER BY val;
-- expected output:
1
2
3
