-- Test vectorized SUBSTRING projection
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'hello world');
INSERT INTO t VALUES (2, 'abcdef');
INSERT INTO t VALUES (3, 'short');
-- input:
SELECT SUBSTRING(name, 1, 5), SUBSTRING(name, 7) FROM t ORDER BY id;
-- expected output:
hello|world
abcde|
short|
