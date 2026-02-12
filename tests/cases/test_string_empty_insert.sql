-- empty string is distinct from NULL
-- setup:
CREATE TABLE t1 (id INT, val TEXT);
INSERT INTO t1 VALUES (1, ''), (2, NULL);
-- input:
SELECT id, val IS NULL AS is_null FROM t1 ORDER BY id;
-- expected output:
1|f
2|t
-- expected status: 0
