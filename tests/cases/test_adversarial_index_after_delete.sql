-- adversarial: index lookup after deleting the indexed row
-- setup:
CREATE TABLE t_iad (id INT PRIMARY KEY, name TEXT);
INSERT INTO t_iad VALUES (1, 'alice');
INSERT INTO t_iad VALUES (2, 'bob');
INSERT INTO t_iad VALUES (3, 'charlie');
CREATE INDEX idx_iad ON t_iad (id);
DELETE FROM t_iad WHERE id = 2;
-- input:
SELECT name FROM t_iad WHERE id = 2;
-- expected output:
