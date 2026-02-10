-- FROM subquery with WHERE filter on outer query
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT * FROM (SELECT id, val FROM t1 WHERE val > 10) AS sub WHERE sub.id < 3;
-- expected output:
2|20
-- expected status: 0
