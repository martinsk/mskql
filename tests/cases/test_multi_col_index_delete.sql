-- multi-column index: DELETE rows, verify index consistency
-- setup:
CREATE TABLE t_mcid (a INT, b INT, val TEXT);
CREATE INDEX idx_mcid_ab ON t_mcid (a, b);
INSERT INTO t_mcid VALUES (1, 10, 'x');
INSERT INTO t_mcid VALUES (1, 20, 'y');
INSERT INTO t_mcid VALUES (2, 10, 'z');
-- input:
DELETE FROM t_mcid WHERE a = 1 AND b = 10;
SELECT val FROM t_mcid WHERE a = 1 AND b = 20;
-- expected output:
DELETE 1
y
