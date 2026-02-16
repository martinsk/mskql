-- AGE function with two timestamp arguments returns an interval
-- setup:
CREATE TABLE t1 (id INT, start_date TIMESTAMP, end_date TIMESTAMP);
INSERT INTO t1 (id, start_date, end_date) VALUES (1, '2024-03-15 00:00:00', '2024-01-15 00:00:00');
-- input:
SELECT AGE(start_date, end_date) FROM t1;
-- expected output:
2 mons
-- expected status: 0
