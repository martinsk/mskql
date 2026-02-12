-- REVERSE function
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'hello'), (2, 'abcde');
-- input:
SELECT id, REVERSE(name) FROM t1 ORDER BY id;
-- expected output:
1|olleh
2|edcba
-- expected status: 0
