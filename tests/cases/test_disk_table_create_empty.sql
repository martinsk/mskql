-- disk table: CREATE DISK TABLE with empty schema, then SELECT
-- setup:
CREATE DISK TABLE t_disk_empty (id INT, name TEXT) DIRECTORY '/tmp/mskql_test_disk_empty';
-- input:
SELECT COUNT(*) FROM t_disk_empty;
-- expected output:
0
