-- BUG: Regex alternation (|) does not work in ~ operator
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'abc'), (2, 'def'), (3, 'ghi');
-- input:
SELECT * FROM t WHERE val ~ 'abc|def' ORDER BY id;
-- expected output:
1|abc
2|def
-- expected status: 0
