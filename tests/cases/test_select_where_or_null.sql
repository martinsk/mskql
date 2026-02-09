-- WHERE with OR and NULL: col = 'x' OR col IS NULL
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'bob'), (4, NULL);
-- input:
SELECT id FROM t1 WHERE name = 'alice' OR name IS NULL ORDER BY id;
-- expected output:
1
2
4
-- expected status: 0
