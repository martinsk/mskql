-- multi-column index: 3-column index
-- setup:
CREATE TABLE t_mci3 (a INT, b INT, c INT, val TEXT);
CREATE INDEX idx_mci3_abc ON t_mci3 (a, b, c);
INSERT INTO t_mci3 VALUES (1, 2, 3, 'found');
INSERT INTO t_mci3 VALUES (1, 2, 4, 'other');
INSERT INTO t_mci3 VALUES (1, 3, 3, 'nope');
-- input:
SELECT val FROM t_mci3 WHERE a = 1 AND b = 2 AND c = 3;
-- expected output:
found
