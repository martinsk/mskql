-- ALTER TABLE ALTER COLUMN TYPE should change column type
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 42);
ALTER TABLE t1 ALTER COLUMN val TYPE TEXT;
-- input:
SELECT id, val FROM t1;
-- expected output:
1|42
-- expected status: 0
