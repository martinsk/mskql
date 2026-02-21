-- BUG: LIKE with custom ESCAPE character does not match correctly
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'a_b'), (2, 'axb'), (3, 'a%b');
-- input:
SELECT * FROM t WHERE val LIKE 'a!_b' ESCAPE '!' ORDER BY id;
-- expected output:
1|a_b
-- expected status: 0
