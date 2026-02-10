-- LOWER function should convert text to lowercase
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'HELLO'), (2, 'World'), (3, NULL);
-- input:
SELECT id, LOWER(name) FROM t1 ORDER BY id;
-- expected output:
1|hello
2|world
3|
-- expected status: 0
