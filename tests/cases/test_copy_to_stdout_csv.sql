-- COPY TO STDOUT WITH CSV
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, 'bob');
-- input:
COPY t1 TO STDOUT WITH CSV
-- expected output:
1,alice
2,bob
-- expected status: 0
