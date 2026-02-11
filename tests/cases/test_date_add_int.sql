-- Date + integer adds days
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-01-15');
-- input:
SELECT d + 10 FROM t1;
-- expected output:
2024-01-25
-- expected status: 0
