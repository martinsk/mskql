-- BUG: Regex dot wildcard (.) does not match any character
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'abc'), (2, 'axc'), (3, 'def');
-- input:
SELECT * FROM t WHERE val ~ 'a.c' ORDER BY id;
-- expected output:
1|abc
2|axc
-- expected status: 0
