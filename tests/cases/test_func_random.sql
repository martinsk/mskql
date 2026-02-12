-- RANDOM() returns a value between 0 and 1
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
-- input:
SELECT CASE WHEN RANDOM() >= 0 AND RANDOM() < 1 THEN 'ok' ELSE 'fail' END FROM t1;
-- expected output:
ok
-- expected status: 0
