-- EXTRACT day from timestamp
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-06-15 10:30:00');
-- input:
SELECT EXTRACT(DAY FROM ts) FROM t1;
-- expected output:
15
-- expected status: 0
