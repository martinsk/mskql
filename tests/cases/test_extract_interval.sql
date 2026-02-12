-- EXTRACT hour from interval
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1);
-- input:
SELECT EXTRACT(HOUR FROM '3 days 05:30:00'::INTERVAL) FROM t1;
-- expected output:
5
-- expected status: 0
