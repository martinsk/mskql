-- SELECT with boolean literal TRUE/FALSE
-- setup:
CREATE TABLE t1 (id INT, active BOOLEAN);
INSERT INTO t1 (id, active) VALUES (1, TRUE), (2, FALSE);
-- input:
SELECT id, active FROM t1 WHERE active = TRUE;
-- expected output:
1|t
-- expected status: 0
