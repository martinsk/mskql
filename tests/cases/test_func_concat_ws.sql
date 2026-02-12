-- CONCAT_WS function
-- setup:
CREATE TABLE t1 (id INT, a TEXT, b TEXT, c TEXT);
INSERT INTO t1 VALUES (1, 'one', 'two', 'three'), (2, 'a', NULL, 'c');
-- input:
SELECT id, CONCAT_WS(', ', a, b, c) FROM t1 ORDER BY id;
-- expected output:
1|one, two, three
2|a, c
-- expected status: 0
