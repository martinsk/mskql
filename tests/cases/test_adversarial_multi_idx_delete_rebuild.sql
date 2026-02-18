-- adversarial: DELETE triggers rebuild, verify remaining rows correct
-- setup:
CREATE TABLE t_amdr (a INT, b INT, val TEXT);
CREATE INDEX idx_amdr ON t_amdr (a, b);
INSERT INTO t_amdr VALUES (1, 1, 'keep1');
INSERT INTO t_amdr VALUES (1, 2, 'del');
INSERT INTO t_amdr VALUES (2, 1, 'keep2');
INSERT INTO t_amdr VALUES (2, 2, 'del2');
-- input:
DELETE FROM t_amdr WHERE b = 2;
SELECT val FROM t_amdr WHERE a = 2 AND b = 1;
-- expected output:
DELETE 2
keep2
