-- DATE_TRUNC to year
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-06-15 10:30:45');
-- input:
SELECT DATE_TRUNC('year', ts) FROM t1;
-- expected output:
2024-01-01 00:00:00
-- expected status: 0
