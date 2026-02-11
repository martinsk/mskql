-- Date + interval adds time
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-01-15 10:00:00');
-- input:
SELECT ts + CAST('2 hours' AS INTERVAL) FROM t1;
-- expected output:
2024-01-15 12:00:00
-- expected status: 0
