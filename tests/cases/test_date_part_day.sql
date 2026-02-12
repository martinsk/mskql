-- DATE_PART function extracts day from date
-- setup:
CREATE TABLE t1 (id INT, d DATE);
INSERT INTO t1 (id, d) VALUES (1, '2024-03-15');
-- input:
SELECT DATE_PART('day', d) FROM t1;
-- expected output:
15
-- expected status: 0
