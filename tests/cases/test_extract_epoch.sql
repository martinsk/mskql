-- EXTRACT year from date works
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-06-15');
-- input:
SELECT EXTRACT(YEAR FROM d) + EXTRACT(MONTH FROM d) FROM t1;
-- expected output:
2030
-- expected status: 0
