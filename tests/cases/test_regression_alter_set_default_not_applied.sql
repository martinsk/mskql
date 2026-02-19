-- bug: ALTER TABLE ALTER COLUMN SET DEFAULT does not apply default on subsequent inserts
-- setup:
CREATE TABLE t_alter_def_apply (id INT, val INT);
INSERT INTO t_alter_def_apply VALUES (1, 10);
ALTER TABLE t_alter_def_apply ALTER COLUMN val SET DEFAULT 99;
INSERT INTO t_alter_def_apply (id) VALUES (2);
-- input:
SELECT * FROM t_alter_def_apply ORDER BY id;
-- expected output:
1|10
2|99
-- expected status: 0
