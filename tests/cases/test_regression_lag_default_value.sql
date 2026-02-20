-- BUG: LAG with default value ignores the default, returns NULL instead
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val, LAG(val, 1, -1) OVER (ORDER BY id) FROM t;
-- expected output:
1|10|-1
2|20|10
3|30|20
-- expected status: 0
