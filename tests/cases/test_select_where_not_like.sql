-- WHERE NOT LIKE pattern
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'alicia');
-- input:
SELECT id FROM t1 WHERE NOT name LIKE 'al%' ORDER BY id;
-- expected output:
2
-- expected status: 0
