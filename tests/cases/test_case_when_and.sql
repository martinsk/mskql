-- CASE WHEN with AND condition
-- setup:
CREATE TABLE t1 (id INT, a INT, b INT);
INSERT INTO t1 (id, a, b) VALUES (1, 10, 20), (2, 5, 20), (3, 10, 5);
-- input:
SELECT id, CASE WHEN a >= 10 AND b >= 10 THEN 'both' ELSE 'no' END FROM t1 ORDER BY id;
-- expected output:
1|both
2|no
3|no
-- expected status: 0
