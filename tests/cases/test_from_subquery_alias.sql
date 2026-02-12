-- FROM subquery with alias
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT sub.id, sub.val FROM (SELECT id, val FROM t1 WHERE val > 10) AS sub ORDER BY sub.id;
-- expected output:
2|20
3|30
-- expected status: 0
