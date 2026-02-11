-- TO_CHAR formats timestamp
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-06-15 10:30:45');
-- input:
SELECT TO_CHAR(ts, 'YYYY-MM-DD') FROM t1;
-- expected output:
2024-06-15
-- expected status: 0
