-- multi-column index: UPDATE indexed column, verify correct results
-- setup:
CREATE TABLE t_mciu (a INT, b INT, val TEXT);
CREATE INDEX idx_mciu_ab ON t_mciu (a, b);
INSERT INTO t_mciu VALUES (1, 10, 'old');
INSERT INTO t_mciu VALUES (2, 20, 'keep');
-- input:
UPDATE t_mciu SET b = 30 WHERE a = 1;
SELECT val FROM t_mciu WHERE a = 1 AND b = 30;
-- expected output:
UPDATE 1
old
