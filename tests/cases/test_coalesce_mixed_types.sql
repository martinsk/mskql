-- COALESCE with first arg NULL, second arg is integer literal
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL), (2, 42);
-- input:
SELECT id, COALESCE(val, 0) FROM t1 ORDER BY id;
-- expected output:
1|0
2|42
-- expected status: 0
