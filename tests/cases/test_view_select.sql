-- CREATE VIEW and SELECT from it
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
CREATE VIEW v1 AS SELECT id, name FROM t1 WHERE id > 0;
-- input:
SELECT * FROM v1 ORDER BY id;
-- expected output:
1|alice
2|bob
-- expected status: 0
