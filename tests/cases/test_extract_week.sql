-- EXTRACT week from date
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-03-15');
-- input:
SELECT EXTRACT(WEEK FROM d) FROM t1;
-- expected output:
11
-- expected status: 0
