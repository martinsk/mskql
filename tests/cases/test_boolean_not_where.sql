-- WHERE NOT on boolean column
-- setup:
CREATE TABLE t1 (id INT, active BOOLEAN);
INSERT INTO t1 VALUES (1, true), (2, false), (3, true);
-- input:
SELECT id FROM t1 WHERE NOT active ORDER BY id;
-- expected output:
2
-- expected status: 0
