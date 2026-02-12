-- INSERT with NOT NULL column that has a DEFAULT should use the default
-- setup:
CREATE TABLE t1 (id SERIAL PRIMARY KEY, name TEXT NOT NULL DEFAULT 'unknown');
-- input:
INSERT INTO t1 (id) VALUES (1);
SELECT id, name FROM t1;
-- expected output:
INSERT 0 1
1|unknown
-- expected status: 0
