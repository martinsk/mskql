-- multi-column index: INSERT multiple rows, verify all findable
-- setup:
CREATE TABLE t_mci2 (a INT, b INT, c TEXT);
CREATE INDEX idx_mci2_ab ON t_mci2 (a, b);
INSERT INTO t_mci2 VALUES (1, 1, 'a');
INSERT INTO t_mci2 VALUES (1, 2, 'b');
INSERT INTO t_mci2 VALUES (2, 1, 'c');
INSERT INTO t_mci2 VALUES (2, 2, 'd');
INSERT INTO t_mci2 VALUES (3, 1, 'e');
-- input:
SELECT c FROM t_mci2 WHERE a = 2 AND b = 1;
-- expected output:
c
