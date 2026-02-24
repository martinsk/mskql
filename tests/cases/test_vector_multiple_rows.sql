-- vector: multiple rows with vector data and WHERE filter
-- setup:
CREATE TABLE t_vmulti (id INT, name TEXT, embedding VECTOR(4));
INSERT INTO t_vmulti VALUES (1, 'alice', '[0.1, 0.2, 0.3, 0.4]');
INSERT INTO t_vmulti VALUES (2, 'bob', '[0.5, 0.6, 0.7, 0.8]');
INSERT INTO t_vmulti VALUES (3, 'carol', '[0.9, 1.0, 1.1, 1.2]');
-- input:
SELECT id, name FROM t_vmulti WHERE id >= 2 ORDER BY id;
-- expected output:
2|bob
3|carol
