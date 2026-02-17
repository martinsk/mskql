-- NOT BETWEEN should exclude NULL rows
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30), (4, NULL);
-- input:
SELECT * FROM t WHERE val NOT BETWEEN 15 AND 25 ORDER BY id;
-- expected output:
1|10
3|30
-- expected status: 0
