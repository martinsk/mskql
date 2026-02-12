-- LEFT with negative n: all but last |n| chars (PostgreSQL semantics)
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, 'hello');
-- input:
SELECT LEFT(val, -2) FROM t1;
-- expected output:
hel
-- expected status: 0
