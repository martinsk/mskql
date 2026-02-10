-- SUBSTRING without length should return rest of string from start position
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, 'hello world');
-- input:
SELECT id, SUBSTRING(val, 7) FROM t1;
-- expected output:
1|world
-- expected status: 0
