-- COALESCE with all NULL arguments should return NULL
-- setup:
CREATE TABLE t1 (id INT, a TEXT, b TEXT);
INSERT INTO t1 (id, a, b) VALUES (1, NULL, NULL);
-- input:
SELECT id, COALESCE(a, b) FROM t1;
-- expected output:
1|
-- expected status: 0
