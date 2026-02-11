-- Timestamp - timestamp returns interval
-- setup:
CREATE TABLE t1 (id INT, ts1 TIMESTAMP, ts2 TIMESTAMP);
INSERT INTO t1 (id, ts1, ts2) VALUES (1, '2024-01-15 10:00:00', '2024-01-10 10:00:00');
-- input:
SELECT ts1 - ts2 FROM t1;
-- expected output:
5 days
-- expected status: 0
