-- adversarial: UNIQUE constraint with multiple NULLs — SQL standard says NULLs are distinct
-- setup:
CREATE TABLE t_un (id INT UNIQUE, name TEXT);
INSERT INTO t_un VALUES (NULL, 'first');
INSERT INTO t_un VALUES (NULL, 'second');
-- input:
SELECT name FROM t_un ORDER BY name;
-- expected output:
first
second
