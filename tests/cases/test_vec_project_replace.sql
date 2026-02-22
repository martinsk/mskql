-- Test vectorized REPLACE projection
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'hello world');
INSERT INTO t VALUES (2, 'foo bar foo');
INSERT INTO t VALUES (3, 'no match');
-- input:
SELECT REPLACE(name, 'foo', 'baz'), REPLACE(name, 'o', 'O') FROM t ORDER BY id;
-- expected output:
hello world|hellO wOrld
baz bar baz|fOO bar fOO
no match|nO match
