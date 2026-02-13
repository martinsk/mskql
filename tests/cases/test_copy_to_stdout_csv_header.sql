-- COPY TO STDOUT WITH CSV HEADER
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice');
INSERT INTO t1 VALUES (2, 'bob');
-- input:
COPY t1 TO STDOUT WITH CSV HEADER
-- expected output:
id,name
1,alice
2,bob
-- expected status: 0
