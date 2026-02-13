-- STRING_AGG with single row (no separator needed)
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
SELECT STRING_AGG(name, ',') FROM t1;
-- expected output:
alice
-- expected status: 0
