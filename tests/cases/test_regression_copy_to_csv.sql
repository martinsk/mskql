-- REGRESSION: COPY TO STDOUT WITH (FORMAT csv) outputs tab-delimited instead of comma-delimited
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'Alice'), (2, 'Bob');
-- input:
COPY t TO STDOUT WITH (FORMAT csv);
-- expected output:
1,Alice
2,Bob
-- expected status: 0
