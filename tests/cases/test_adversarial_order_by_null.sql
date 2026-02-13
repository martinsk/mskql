-- adversarial: ORDER BY column with NULLs — tests NULL ordering
-- setup:
CREATE TABLE t_obn (id INT, name TEXT);
INSERT INTO t_obn VALUES (1, 'charlie');
INSERT INTO t_obn VALUES (2, NULL);
INSERT INTO t_obn VALUES (3, 'alice');
INSERT INTO t_obn VALUES (4, NULL);
INSERT INTO t_obn VALUES (5, 'bob');
-- input:
SELECT id, name FROM t_obn ORDER BY name;
-- expected output:
3|alice
5|bob
1|charlie
2|
4|
