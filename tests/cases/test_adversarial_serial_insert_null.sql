-- adversarial: SERIAL column with explicit NULL insert
-- setup:
CREATE TABLE t_sn (id SERIAL, name TEXT);
INSERT INTO t_sn (name) VALUES ('first');
INSERT INTO t_sn (name) VALUES ('second');
-- input:
SELECT id, name FROM t_sn ORDER BY id;
-- expected output:
1|first
2|second
