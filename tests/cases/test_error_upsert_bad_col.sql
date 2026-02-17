-- ON CONFLICT DO UPDATE SET with nonexistent column should error
-- setup:
CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET nonexistent = 'x';
-- expected output:
ERROR:  column "nonexistent" of relation "t1" does not exist
-- expected status: 1
