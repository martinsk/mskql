-- SUBSTRING(str FROM start FOR length) basic usage
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, 'hello world'), (2, 'abcdef');
-- input:
SELECT id, SUBSTRING(val, 1, 5) FROM t1 ORDER BY id;
-- expected output:
1|hello
2|abcde
-- expected status: 0
