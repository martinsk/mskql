-- NOT (val > x) should exclude NULL rows (NULL is not true or false)
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, NULL), (3, 20);
-- input:
SELECT * FROM t WHERE NOT (val > 15) ORDER BY id;
-- expected output:
1|10
-- expected status: 0
