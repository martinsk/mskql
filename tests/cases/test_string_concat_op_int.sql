-- || operator with integer operand should cast to text
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'item');
-- input:
SELECT name || id FROM t1;
-- expected output:
item1
-- expected status: 0
