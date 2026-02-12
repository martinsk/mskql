-- DENSE_RANK window function
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 VALUES (1, 100), (2, 100), (3, 90), (4, 80);
-- input:
SELECT id, score, DENSE_RANK() OVER (ORDER BY score DESC) AS drank FROM t1 ORDER BY id;
-- expected output:
1|100|1
2|100|1
3|90|2
4|80|3
-- expected status: 0
