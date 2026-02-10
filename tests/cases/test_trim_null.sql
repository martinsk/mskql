-- TRIM on NULL should return NULL
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 (id, val) VALUES (1, NULL), (2, ' ok ');
-- input:
SELECT id, TRIM(val) FROM t1 ORDER BY id;
-- expected output:
1|
2|ok
-- expected status: 0
