-- multi-column index: IF NOT EXISTS with multi-column index
-- setup:
CREATE TABLE t_mcine (a INT, b INT, val TEXT);
CREATE INDEX idx_mcine_ab ON t_mcine (a, b);
CREATE INDEX IF NOT EXISTS idx_mcine_ab ON t_mcine (a, b);
INSERT INTO t_mcine VALUES (1, 2, 'ok');
-- input:
SELECT val FROM t_mcine WHERE a = 1 AND b = 2;
-- expected output:
ok
