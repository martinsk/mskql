-- Nested function calls: UPPER(TRIM(name))
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, '  hello  '), (2, ' World ');
-- input:
SELECT id, UPPER(TRIM(name)) FROM t1 ORDER BY id;
-- expected output:
1|HELLO
2|WORLD
-- expected status: 0
