-- EXTRACT hour from timestamp
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-06-15 10:30:45');
-- input:
SELECT EXTRACT(HOUR FROM ts) FROM t1;
-- expected output:
10
-- expected status: 0
