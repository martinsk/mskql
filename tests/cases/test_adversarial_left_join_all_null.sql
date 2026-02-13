-- adversarial: LEFT JOIN where right side has no matches — all NULLs
-- setup:
CREATE TABLE t_lj1 (id INT, name TEXT);
CREATE TABLE t_lj2 (id INT, value INT);
INSERT INTO t_lj1 VALUES (1, 'alice');
INSERT INTO t_lj1 VALUES (2, 'bob');
INSERT INTO t_lj2 VALUES (99, 100);
-- input:
SELECT t_lj1.name, t_lj2.value FROM t_lj1 LEFT JOIN t_lj2 ON t_lj1.id = t_lj2.id ORDER BY t_lj1.name;
-- expected output:
alice|
bob|
