-- DISTINCT with ORDER BY should deduplicate then sort
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 30), (2, 10), (3, 30), (4, 20), (5, 10);
-- input:
SELECT DISTINCT val FROM t1 ORDER BY val;
-- expected output:
10
20
30
-- expected status: 0
