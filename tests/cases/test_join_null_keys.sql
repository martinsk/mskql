-- JOIN on columns with NULL values - NULLs should not match
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
CREATE TABLE t2 (id INT, label TEXT);
INSERT INTO t1 VALUES (1, 'a'), (NULL, 'b'), (3, 'c');
INSERT INTO t2 VALUES (1, 'x'), (NULL, 'y'), (3, 'z');
-- input:
SELECT t1.val, t2.label FROM t1 INNER JOIN t2 ON t1.id = t2.id ORDER BY t1.val;
-- expected output:
a|x
c|z
-- expected status: 0
