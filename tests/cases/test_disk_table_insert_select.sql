-- disk table: INSERT rows into disk table then SELECT them back
-- setup:
CREATE DISK TABLE t_disk_ins (id INT, name TEXT, score FLOAT) DIRECTORY '/tmp/mskql_test_disk_ins';
INSERT INTO t_disk_ins VALUES (1, 'alice', 95.5);
INSERT INTO t_disk_ins VALUES (2, 'bob', 87.0);
INSERT INTO t_disk_ins VALUES (3, 'carol', 91.2);
-- input:
SELECT id, name, score FROM t_disk_ins ORDER BY id;
-- expected output:
1|alice|95.5
2|bob|87
3|carol|91.2
