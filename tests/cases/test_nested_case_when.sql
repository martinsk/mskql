-- nested CASE WHEN expressions
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, CASE WHEN val > 25 THEN 'high' WHEN val > 15 THEN CASE WHEN id = 2 THEN 'mid-a' ELSE 'mid-b' END ELSE 'low' END AS label FROM t1 ORDER BY id;
-- expected output:
1|low
2|mid-a
3|high
-- expected status: 0
