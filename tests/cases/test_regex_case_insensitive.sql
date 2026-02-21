-- BUG: Case-insensitive regex operator ~* does not match case-insensitively
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'hello'), (2, 'HELLO'), (3, 'world');
-- input:
SELECT * FROM t WHERE val ~* 'hello' ORDER BY id;
-- expected output:
1|hello
2|HELLO
-- expected status: 0
