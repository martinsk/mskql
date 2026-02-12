-- DATE_TRUNC to minute
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-06-15 14:35:22');
-- input:
SELECT DATE_TRUNC('minute', ts) FROM t1;
-- expected output:
2024-06-15 14:35:00
-- expected status: 0
