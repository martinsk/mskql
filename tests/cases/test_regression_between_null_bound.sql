-- BETWEEN with NULL bound should return no rows (comparison with NULL yields NULL)
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, NULL), (3, 20);
-- input:
SELECT * FROM t WHERE val BETWEEN NULL AND 20 ORDER BY id;
-- expected output:
-- expected status: 0
