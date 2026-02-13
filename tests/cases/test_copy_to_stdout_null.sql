-- COPY TO STDOUT with NULL values
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, NULL);
INSERT INTO t1 VALUES (3, 'carol');
-- input:
COPY t1 TO STDOUT
-- expected output:
1	alice
2	\N
3	carol
-- expected status: 0
