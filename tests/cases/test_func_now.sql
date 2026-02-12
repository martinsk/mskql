-- NOW() returns a timestamp string
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
-- input:
SELECT CASE WHEN LENGTH(NOW()::TEXT) >= 19 THEN 'ok' ELSE 'fail' END FROM t1;
-- expected output:
ok
-- expected status: 0
