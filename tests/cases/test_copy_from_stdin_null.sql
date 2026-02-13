-- COPY TO STDOUT with boolean and smallint types
-- setup:
CREATE TABLE t1 (id INT, flag BOOLEAN);
INSERT INTO t1 VALUES (1, true);
INSERT INTO t1 VALUES (2, false);
-- input:
COPY t1 TO STDOUT
-- expected output:
1	t
2	f
-- expected status: 0
