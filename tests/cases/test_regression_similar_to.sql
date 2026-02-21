-- BUG: SIMILAR TO operator not supported in WHERE clause
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'abc'), (2, 'def'), (3, 'abcdef');
-- input:
SELECT * FROM t WHERE val SIMILAR TO 'abc%' ORDER BY id;
-- expected output:
1|abc
3|abcdef
-- expected status: 0
