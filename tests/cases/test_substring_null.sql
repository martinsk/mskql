-- SUBSTRING on NULL should return NULL
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, 'hello'), (2, NULL);
-- input:
SELECT id, SUBSTRING(val, 1, 3) FROM t1 ORDER BY id;
-- expected output:
1|hel
2|
-- expected status: 0
