-- LEFT JOIN with NULL keys - NULL keys should not match but left rows preserved
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
CREATE TABLE t2 (id INT, label TEXT);
INSERT INTO t1 VALUES (1, 'a'), (NULL, 'b');
INSERT INTO t2 VALUES (1, 'x'), (NULL, 'y');
-- input:
SELECT t1.val, t2.label FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t1.val;
-- expected output:
a|x
b|
-- expected status: 0
