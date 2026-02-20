-- BUG: INSERT with more values than specified columns should error
-- setup:
CREATE TABLE t (id INT, name TEXT, val INT);
-- input:
INSERT INTO t (id, name) VALUES (1, 'a', 100);
-- expected output:
ERROR:  INSERT has more expressions than target columns
-- expected status: 0
