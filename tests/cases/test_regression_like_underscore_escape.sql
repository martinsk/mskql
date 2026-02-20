-- BUG: LIKE with escaped underscore \_  does not match literal underscore
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'hello%world'), (2, 'hello_world'), (3, 'helloXworld');
-- input:
SELECT * FROM t WHERE val LIKE 'hello\_world';
-- expected output:
2|hello_world
-- expected status: 0
