-- disk table: DROP TABLE removes disk table
-- setup:
CREATE DISK TABLE t_disk_drop (id INT) DIRECTORY '/tmp/mskql_test_disk_drop';
INSERT INTO t_disk_drop VALUES (1), (2);
DROP TABLE t_disk_drop;
-- input:
SELECT COUNT(*) FROM t_disk_drop;
-- expected output:
ERROR:  table 't_disk_drop' not found
-- expected status: non-zero
