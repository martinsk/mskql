-- BUG: Regex character classes like [0-9] and [a-z] do not work in ~ operator
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'abc123'), (2, 'xyz'), (3, '123abc');
-- input:
SELECT * FROM t WHERE val ~ '[0-9]' ORDER BY id;
-- expected output:
1|abc123
3|123abc
-- expected status: 0
