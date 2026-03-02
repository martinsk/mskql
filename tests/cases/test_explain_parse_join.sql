-- explain parse: SELECT with JOIN and WHERE
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
CREATE TABLE t2 (fk INT, score INT);
INSERT INTO t1 VALUES (1,'alpha'),(2,'beta'),(3,'gamma');
INSERT INTO t2 VALUES (1,10),(2,20),(3,30);
-- input:
EXPLAIN (PARSE) SELECT t1.val, t2.score FROM t1 JOIN t2 ON t1.id = t2.fk WHERE t2.score > 15
-- expected output:
Parse AST (SELECT)
  FROM:      t1
  COLUMNS:   t1.val, t2.score
  WHERE:     t2.score > 15
  JOIN:      t2 (INNER)
-- expected status: 0
