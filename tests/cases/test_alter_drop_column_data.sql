-- ALTER TABLE DROP COLUMN removes data from existing rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT, age INT);
INSERT INTO t1 VALUES (1, 'alice', 30), (2, 'bob', 25);
ALTER TABLE t1 DROP COLUMN name;
-- input:
SELECT id, age FROM t1 ORDER BY id;
-- expected output:
1|30
2|25
-- expected status: 0
