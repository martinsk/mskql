-- Window COUNT(*) OVER (PARTITION BY dept) should count all rows in partition
-- setup:
CREATE TABLE t1 (id INT, dept TEXT);
INSERT INTO t1 (id, dept) VALUES (1, 'eng'), (2, 'eng'), (3, 'sales');
-- input:
SELECT id, dept, COUNT(*) OVER (PARTITION BY dept) FROM t1 ORDER BY id;
-- expected output:
1|eng|2
2|eng|2
3|sales|1
-- expected status: 0
