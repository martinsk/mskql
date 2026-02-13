-- adversarial: index lookup after updating the indexed column
-- setup:
CREATE TABLE t_iau (id INT, name TEXT);
INSERT INTO t_iau VALUES (1, 'alice');
INSERT INTO t_iau VALUES (2, 'bob');
CREATE INDEX idx_iau ON t_iau (id);
UPDATE t_iau SET id = 99 WHERE name = 'bob';
-- input:
SELECT name FROM t_iau WHERE id = 99;
-- expected output:
bob
