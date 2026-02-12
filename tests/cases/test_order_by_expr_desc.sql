-- ORDER BY expression with DESC
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 VALUES (1, 3, 4), (2, 5, 1), (3, 1, 2);
-- input:
SELECT id, a * b AS product FROM t1 ORDER BY a * b DESC;
-- expected output:
1|12
2|5
3|2
-- expected status: 0
