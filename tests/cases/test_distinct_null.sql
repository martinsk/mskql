-- DISTINCT should deduplicate rows with same non-null values when NULLs are present
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 10), (3, 20), (4, 20);
-- input:
SELECT DISTINCT val FROM t1 ORDER BY val;
-- expected output:
10
20
-- expected status: 0
