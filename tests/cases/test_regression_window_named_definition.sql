-- BUG: Named window definitions (WINDOW w AS (...)) not supported
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT id, val, SUM(val) OVER w FROM t WINDOW w AS (ORDER BY id);
-- expected output:
1|10|10
2|20|30
3|30|60
-- expected status: 0
