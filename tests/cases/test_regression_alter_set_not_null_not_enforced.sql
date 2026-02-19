-- bug: ALTER TABLE ALTER COLUMN SET NOT NULL does not enforce constraint on subsequent inserts
-- setup:
CREATE TABLE t_alter_nn_enforce (id INT, val INT);
INSERT INTO t_alter_nn_enforce VALUES (1, 10);
ALTER TABLE t_alter_nn_enforce ALTER COLUMN val SET NOT NULL;
-- input:
INSERT INTO t_alter_nn_enforce VALUES (2, NULL);
-- expected output:
ERROR:  NOT NULL constraint violated for column 'val'
-- expected status: 0
