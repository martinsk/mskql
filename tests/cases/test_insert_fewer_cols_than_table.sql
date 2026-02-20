-- INSERT with fewer values than columns should error (PostgreSQL behavior)
-- setup:
CREATE TABLE t1 (id INT, name TEXT, age INT);
-- input:
INSERT INTO t1 VALUES (1, 'alice');
-- expected output:
ERROR:  INSERT has fewer expressions than target columns
-- expected status: 0
