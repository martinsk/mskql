-- EXTRACT day of week from date
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-01-01');
-- input:
SELECT EXTRACT(DOW FROM d) FROM t1;
-- expected output:
1
-- expected status: 0
