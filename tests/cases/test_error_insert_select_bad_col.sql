-- INSERT...SELECT with nonexistent column in column list should error
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
-- input:
INSERT INTO t1 (nonexistent) SELECT 1;
-- expected output:
ERROR:  column "nonexistent" of relation "t1" does not exist
-- expected status: 1
