-- INSERT without VALUES or SELECT should report specific error
-- setup:
CREATE TABLE t1 (id INT);
-- input:
INSERT INTO t1 (id) (1);
-- expected output:
ERROR:  expected VALUES or SELECT
-- expected status: 1
