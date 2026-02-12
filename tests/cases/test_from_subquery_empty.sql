-- FROM subquery that returns no rows
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 10);
-- input:
SELECT * FROM (SELECT id, val FROM t1 WHERE val > 100) AS sub;
-- expected output:
-- expected status: 0
