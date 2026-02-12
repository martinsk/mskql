-- FULL OUTER JOIN with NULL keys
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
CREATE TABLE t2 (id INT, label TEXT);
INSERT INTO t1 VALUES (1, 'a'), (NULL, 'b');
INSERT INTO t2 VALUES (1, 'x'), (NULL, 'y');
-- input:
SELECT t1.val, t2.label FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id ORDER BY t1.val, t2.label;
-- expected output:
a|x
b|
|y
-- expected status: 0
