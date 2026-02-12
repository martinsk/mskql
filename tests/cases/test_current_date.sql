-- NOW() returns a timestamp string
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
-- input:
SELECT LENGTH(NOW()::TEXT) FROM t1;
-- expected output:
19
-- expected status: 0
