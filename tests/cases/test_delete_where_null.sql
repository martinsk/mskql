-- DELETE rows WHERE column IS NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'charlie'), (4, NULL);
-- input:
DELETE FROM t1 WHERE name IS NULL;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
DELETE 2
1|alice
3|charlie
-- expected status: 0
