-- BUG: INSERT with fewer values than table columns (no column list) should error
-- setup:
CREATE TABLE t (id INT, name TEXT, val INT);
-- input:
INSERT INTO t VALUES (1);
-- expected output:
ERROR:  INSERT has fewer expressions than target columns
-- expected status: 0
