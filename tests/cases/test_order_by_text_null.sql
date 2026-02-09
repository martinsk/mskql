-- ORDER BY on text column with NULLs
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'charlie'), (2, NULL), (3, 'alice'), (4, NULL);
-- input:
SELECT id, name FROM t1 ORDER BY name;
-- expected output:
3|alice
1|charlie
2|
4|
-- expected status: 0
