-- DATE_PART function extracts month from timestamp
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-09-20 14:30:00');
-- input:
SELECT DATE_PART('month', ts) FROM t1;
-- expected output:
9
-- expected status: 0
