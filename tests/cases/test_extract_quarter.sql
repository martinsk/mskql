-- EXTRACT quarter from date
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-06-15');
INSERT INTO t1 (id, d) VALUES (2, '2024-01-15');
INSERT INTO t1 (id, d) VALUES (3, '2024-11-15');
-- input:
SELECT id, EXTRACT(QUARTER FROM d) FROM t1 ORDER BY id;
-- expected output:
1|2
2|1
3|4
-- expected status: 0
