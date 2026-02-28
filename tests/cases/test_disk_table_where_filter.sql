-- disk table: WHERE filter on disk table
-- setup:
CREATE DISK TABLE t_disk_where (id INT, name TEXT, active BOOLEAN) DIRECTORY '/tmp/mskql_test_disk_where';
INSERT INTO t_disk_where VALUES (1, 'alice', true);
INSERT INTO t_disk_where VALUES (2, 'bob', false);
INSERT INTO t_disk_where VALUES (3, 'carol', true);
-- input:
SELECT id, name FROM t_disk_where WHERE active = true ORDER BY id;
-- expected output:
1|alice
3|carol
