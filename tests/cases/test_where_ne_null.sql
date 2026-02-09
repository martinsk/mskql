-- WHERE col != value should NOT return rows where col is NULL (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, NULL), (3, 'bob');
-- input:
SELECT id FROM t1 WHERE name != 'alice' ORDER BY id;
-- expected output:
3
-- expected status: 0
