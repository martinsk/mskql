-- self join with less-than to find ordered pairs
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, 10), (2, 20), (3, 30);
-- input:
SELECT a.id, b.id FROM t1 AS a JOIN t1 AS b ON a.val < b.val ORDER BY a.id, b.id;
-- expected output:
1|2
1|3
2|3
-- expected status: 0
