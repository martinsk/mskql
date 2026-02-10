-- UNION ALL should preserve duplicates (unlike UNION)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
CREATE TABLE t2 (id INT, name TEXT);
INSERT INTO t2 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
SELECT id, name FROM t1 UNION ALL SELECT id, name FROM t2 ORDER BY id;
-- expected output:
1|alice
1|alice
2|bob
-- expected status: 0
