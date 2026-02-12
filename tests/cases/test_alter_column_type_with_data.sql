-- ALTER TABLE ALTER COLUMN TYPE changes type but data stays as-is
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 42), (2, 99);
ALTER TABLE t1 ALTER COLUMN val TYPE BIGINT;
-- input:
SELECT id, val FROM t1 ORDER BY id;
-- expected output:
1|42
2|99
-- expected status: 0
