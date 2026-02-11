-- EXTRACT minute and second from timestamp
-- setup:
CREATE TABLE t1 (id INT, ts TIMESTAMP);
INSERT INTO t1 (id, ts) VALUES (1, '2024-06-15 10:30:45');
-- input:
SELECT EXTRACT(MINUTE FROM ts), EXTRACT(SECOND FROM ts) FROM t1;
-- expected output:
30|45
-- expected status: 0
