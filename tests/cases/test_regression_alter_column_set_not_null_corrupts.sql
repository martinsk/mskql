-- bug: ALTER TABLE ALTER COLUMN SET NOT NULL corrupts existing text data
-- setup:
CREATE TABLE t_alter_nn (id INT, name TEXT);
INSERT INTO t_alter_nn VALUES (1, 'hello'), (2, 'world');
ALTER TABLE t_alter_nn ALTER COLUMN name SET NOT NULL;
-- input:
SELECT * FROM t_alter_nn ORDER BY id;
-- expected output:
1|hello
2|world
-- expected status: 0
