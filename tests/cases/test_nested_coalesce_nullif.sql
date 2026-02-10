-- nested function: COALESCE(NULLIF(val, 0), -1) should replace 0 with -1
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 5), (2, 0), (3, NULL);
-- input:
SELECT id, COALESCE(NULLIF(val, 0), -1) FROM t1 ORDER BY id;
-- expected output:
1|5
2|-1
3|-1
-- expected status: 0
