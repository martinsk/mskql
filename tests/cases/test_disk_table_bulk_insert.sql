-- disk table: bulk INSERT and aggregates on disk table
-- setup:
CREATE DISK TABLE t_disk_bulk (id INT, val INT) DIRECTORY '/tmp/mskql_test_disk_bulk';
INSERT INTO t_disk_bulk VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT COUNT(*), SUM(val), MIN(val), MAX(val) FROM t_disk_bulk;
-- expected output:
5|150|10|50
