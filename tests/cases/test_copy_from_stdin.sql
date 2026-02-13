-- COPY TO STDOUT with multiple types
-- setup:
CREATE TABLE t1 (id INT, name TEXT, score FLOAT);
INSERT INTO t1 VALUES (1, 'alice', 9.5);
INSERT INTO t1 VALUES (2, 'bob', 8.3);
-- input:
COPY t1 TO STDOUT
-- expected output:
1	alice	9.5
2	bob	8.3
-- expected status: 0
