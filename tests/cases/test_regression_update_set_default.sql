-- BUG: UPDATE SET col = DEFAULT sets to NULL instead of column's default value
-- setup:
CREATE TABLE t (id INT, name TEXT DEFAULT 'default_name');
INSERT INTO t VALUES (1, 'custom');
UPDATE t SET name = DEFAULT WHERE id = 1;
-- input:
SELECT * FROM t;
-- expected output:
1|default_name
-- expected status: 0
