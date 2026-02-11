-- SERIAL should not reuse IDs after ON CONFLICT DO NOTHING skips a row
-- setup:
CREATE TABLE t1 (id SERIAL, name TEXT UNIQUE);
INSERT INTO t1 (name) VALUES ('alice');
INSERT INTO t1 (name) VALUES ('alice') ON CONFLICT DO NOTHING;
INSERT INTO t1 (name) VALUES ('bob');
-- input:
SELECT * FROM t1 ORDER BY id;
-- expected output:
1|alice
3|bob
-- expected status: 0
