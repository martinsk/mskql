-- WHERE on boolean column without explicit comparison
-- setup:
CREATE TABLE t1 (id INT, active BOOLEAN);
INSERT INTO t1 VALUES (1, true), (2, false), (3, true);
-- input:
SELECT id FROM t1 WHERE active ORDER BY id;
-- expected output:
1
3
-- expected status: 0
