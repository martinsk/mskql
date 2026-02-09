-- ALTER TABLE DROP COLUMN then INSERT should work with remaining columns
-- setup:
CREATE TABLE t1 (id INT, name TEXT, age INT);
INSERT INTO t1 (id, name, age) VALUES (1, 'alice', 30);
ALTER TABLE t1 DROP COLUMN age;
-- input:
INSERT INTO t1 (id, name) VALUES (2, 'bob');
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
INSERT 0 1
1|alice
2|bob
-- expected status: 0
