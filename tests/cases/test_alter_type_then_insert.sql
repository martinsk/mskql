-- ALTER COLUMN TYPE then INSERT with new type should work
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 42);
ALTER TABLE t1 ALTER COLUMN val TYPE TEXT;
-- input:
INSERT INTO t1 (id, val) VALUES (2, 'hello');
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
INSERT 0 1
1|42
2|hello
-- expected status: 0
