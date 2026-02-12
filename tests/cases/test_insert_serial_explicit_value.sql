-- INSERT with explicit value for SERIAL column should update serial_next
-- setup:
CREATE TABLE t1 (id SERIAL PRIMARY KEY, name TEXT);
INSERT INTO t1 (id, name) VALUES (10, 'alice');
-- input:
INSERT INTO t1 (name) VALUES ('bob');
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
INSERT 0 1
10|alice
11|bob
-- expected status: 0
