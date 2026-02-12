-- EXTRACT day of year from date
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-02-15');
-- input:
SELECT EXTRACT(DOY FROM d) FROM t1;
-- expected output:
46
-- expected status: 0
