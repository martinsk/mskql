-- INSERT violating NOT NULL constraint should fail
-- setup:
CREATE TABLE t1 (id INT, name TEXT NOT NULL);
-- input:
INSERT INTO t1 (id, name) VALUES (1, NULL);
-- expected status: 1
