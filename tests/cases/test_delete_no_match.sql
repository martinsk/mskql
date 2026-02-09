-- DELETE with WHERE that matches no rows should report 0 deleted
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
DELETE FROM t1 WHERE id = 999;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
DELETE 0
1|alice
2|bob
-- expected status: 0
