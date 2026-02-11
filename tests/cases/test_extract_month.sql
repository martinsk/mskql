-- EXTRACT month from date
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-06-15');
-- input:
SELECT EXTRACT(MONTH FROM d) FROM t1;
-- expected output:
6
-- expected status: 0
