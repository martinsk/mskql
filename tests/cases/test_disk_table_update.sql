-- disk table: UPDATE rows in disk table
-- setup:
CREATE DISK TABLE t_disk_upd (id INT, name TEXT, score INT) DIRECTORY '/tmp/mskql_test_disk_upd';
INSERT INTO t_disk_upd VALUES (1, 'alice', 80), (2, 'bob', 70), (3, 'carol', 90);
UPDATE t_disk_upd SET score = 100 WHERE id = 2;
-- input:
SELECT id, name, score FROM t_disk_upd ORDER BY id;
-- expected output:
1|alice|80
2|bob|100
3|carol|90
