-- disk table: DELETE rows from disk table
-- setup:
CREATE DISK TABLE t_disk_del (id INT, name TEXT) DIRECTORY '/tmp/mskql_test_disk_del';
INSERT INTO t_disk_del VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');
DELETE FROM t_disk_del WHERE id = 2;
-- input:
SELECT id, name FROM t_disk_del ORDER BY id;
-- expected output:
1|alice
3|carol
