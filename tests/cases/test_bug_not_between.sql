-- bug: NOT BETWEEN fails with "expected IN after NOT"
-- setup:
CREATE TABLE t_nb (id INT, val INT);
INSERT INTO t_nb VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT id, val FROM t_nb WHERE val NOT BETWEEN 20 AND 40 ORDER BY id;
-- expected output:
1|10
5|50
