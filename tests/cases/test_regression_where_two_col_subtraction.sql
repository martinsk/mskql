-- BUG: WHERE clause with two-column subtraction comparison returns wrong results
-- setup:
CREATE TABLE t (id INT, a INT, b INT);
INSERT INTO t VALUES (1, 10, 5), (2, 20, 15), (3, 30, 25), (4, 50, 10);
-- input:
SELECT * FROM t WHERE a - b = 5 ORDER BY id;
-- expected output:
1|10|5
2|20|15
3|30|25
-- expected status: 0
