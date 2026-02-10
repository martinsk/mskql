-- TRIM should remove leading and trailing whitespace
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, '  hello  '), (2, 'world'), (3, '  spaces  ');
-- input:
SELECT id, TRIM(val) FROM t1 ORDER BY id;
-- expected output:
1|hello
2|world
3|spaces
-- expected status: 0
