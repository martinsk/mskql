-- LIKE with _ should match exactly one character
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'a'), (2, 'ab'), (3, ''), (4, 'x');
-- input:
SELECT id FROM t1 WHERE name LIKE '_' ORDER BY id;
-- expected output:
1
4
-- expected status: 0
