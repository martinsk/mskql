-- String concatenation with NULL should return NULL (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, NULL);
-- input:
SELECT id, 'hello' || name FROM t1;
-- expected output:
1|
-- expected status: 0
