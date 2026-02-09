-- UPDATE rows WHERE column IS NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, NULL), (3, NULL);
-- input:
UPDATE t1 SET name = 'unknown' WHERE name IS NULL;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
UPDATE 2
1|alice
2|unknown
3|unknown
-- expected status: 0
