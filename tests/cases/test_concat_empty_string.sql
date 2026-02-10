-- string concat with empty string should work correctly
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'hello'), (2, '');
-- input:
SELECT id, name || '!' FROM t1 ORDER BY id;
-- expected output:
1|hello!
2|!
-- expected status: 0
