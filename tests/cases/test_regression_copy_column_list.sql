-- REGRESSION: COPY with column list not supported
-- setup:
CREATE TABLE t (id INT, name TEXT, val INT);
INSERT INTO t VALUES (1, 'Alice', 100), (2, 'Bob', 200);
-- input:
COPY t (id, name) TO STDOUT;
-- expected output:
1	Alice
2	Bob
-- expected status: 0
