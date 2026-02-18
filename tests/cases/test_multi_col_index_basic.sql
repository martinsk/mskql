-- multi-column index: basic CREATE INDEX on (a, b), INSERT, SELECT
-- setup:
CREATE TABLE t_mci (a INT, b INT, val TEXT);
INSERT INTO t_mci VALUES (1, 10, 'x');
INSERT INTO t_mci VALUES (1, 20, 'y');
INSERT INTO t_mci VALUES (2, 10, 'z');
INSERT INTO t_mci VALUES (2, 20, 'w');
CREATE INDEX idx_mci_ab ON t_mci (a, b);
-- input:
SELECT val FROM t_mci WHERE a = 1 AND b = 20;
-- expected output:
y
