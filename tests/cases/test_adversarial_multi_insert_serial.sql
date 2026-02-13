-- adversarial: multiple inserts with SERIAL — tests auto-increment consistency
-- setup:
CREATE TABLE t_mis (id SERIAL, name TEXT);
INSERT INTO t_mis (name) VALUES ('a');
INSERT INTO t_mis (name) VALUES ('b');
INSERT INTO t_mis (name) VALUES ('c');
DELETE FROM t_mis WHERE name = 'b';
INSERT INTO t_mis (name) VALUES ('d');
-- input:
SELECT id, name FROM t_mis ORDER BY id;
-- expected output:
1|a
3|c
4|d
