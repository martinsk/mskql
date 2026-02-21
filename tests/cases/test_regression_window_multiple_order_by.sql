-- BUG: Window function with multiple ORDER BY columns in OVER clause fails with parse error
-- setup:
CREATE TABLE t (id INT, name TEXT, val INT);
INSERT INTO t VALUES (1, 'c', 30), (2, 'a', 10), (3, 'b', 20);
-- input:
SELECT id, name, val, ROW_NUMBER() OVER (ORDER BY val DESC, name) AS rn FROM t ORDER BY id;
-- expected output:
1|c|30|1
2|a|10|3
3|b|20|2
-- expected status: 0
