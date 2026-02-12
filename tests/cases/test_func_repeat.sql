-- REPEAT function
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'ab'), (2, 'x');
-- input:
SELECT id, REPEAT(name, 3) FROM t1 ORDER BY id;
-- expected output:
1|ababab
2|xxx
-- expected status: 0
