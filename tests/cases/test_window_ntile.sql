-- NTILE window function
-- setup:
CREATE TABLE t1 (id INT);
INSERT INTO t1 VALUES (1), (2), (3), (4), (5);
-- input:
SELECT id, NTILE(3) OVER (ORDER BY id) AS bucket FROM t1 ORDER BY id;
-- expected output:
1|1
2|1
3|2
4|2
5|3
-- expected status: 0
