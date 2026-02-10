-- nested COALESCE with UPPER: COALESCE(UPPER(name), 'N/A')
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'hello'), (2, NULL);
-- input:
SELECT id, COALESCE(UPPER(name), 'N/A') FROM t1 ORDER BY id;
-- expected output:
1|HELLO
2|N/A
-- expected status: 0
