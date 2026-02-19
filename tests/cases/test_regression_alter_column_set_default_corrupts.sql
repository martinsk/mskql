-- bug: ALTER TABLE ALTER COLUMN SET DEFAULT corrupts existing text data
-- setup:
CREATE TABLE t_alter_def (id INT, name TEXT);
INSERT INTO t_alter_def VALUES (1, 'hello'), (2, 'world');
ALTER TABLE t_alter_def ALTER COLUMN name SET DEFAULT 'unknown';
-- input:
SELECT * FROM t_alter_def ORDER BY id;
-- expected output:
1|hello
2|world
-- expected status: 0
