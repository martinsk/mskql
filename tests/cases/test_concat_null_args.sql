-- CONCAT with NULL arguments treats them as empty string
-- setup:
CREATE TABLE t1 (id INT, a TEXT, b TEXT);
INSERT INTO t1 VALUES (1, 'hello', NULL), (2, NULL, 'world'), (3, NULL, NULL);
-- input:
SELECT id, CONCAT(a, b) FROM t1 ORDER BY id;
-- expected output:
1|hello
2|world
3|
-- expected status: 0
