-- IS NOT NULL in SELECT expression list
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, NULL);
-- input:
SELECT id, name IS NOT NULL FROM t1 ORDER BY id;
-- expected output:
1|t
2|f
-- expected status: 0
