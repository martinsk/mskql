-- IS NULL / IS NOT NULL in SELECT expression list
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, NULL);
-- input:
SELECT id, name IS NULL FROM t1 ORDER BY id;
-- expected output:
1|f
2|t
-- expected status: 0
