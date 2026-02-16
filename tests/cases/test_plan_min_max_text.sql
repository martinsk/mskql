-- Test MIN/MAX on TEXT columns through plan executor
-- setup:
CREATE TABLE t_mmt (id INT, name TEXT);
INSERT INTO t_mmt VALUES (1, 'charlie');
INSERT INTO t_mmt VALUES (2, 'alice');
INSERT INTO t_mmt VALUES (3, NULL);
INSERT INTO t_mmt VALUES (4, 'bob');
INSERT INTO t_mmt VALUES (5, 'dave');
-- input:
SELECT MIN(name), MAX(name) FROM t_mmt;
-- expected output:
alice|dave
