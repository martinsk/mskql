-- string concat with NULL column should produce NULL (SQL standard: NULL || 'x' = NULL)
-- setup:
CREATE TABLE t1 (id INT, first_name TEXT, last_name TEXT);
INSERT INTO t1 (id, first_name, last_name) VALUES (1, 'Alice', NULL), (2, NULL, 'Jones');
-- input:
SELECT id, first_name || ' ' || last_name FROM t1 ORDER BY id;
-- expected output:
1|
2|
-- expected status: 0
