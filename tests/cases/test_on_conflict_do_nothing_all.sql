-- ON CONFLICT DO NOTHING where all rows conflict should insert 0
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'x'), (2, 'y') ON CONFLICT DO NOTHING;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
INSERT 0 0
1|alice
2|bob
-- expected status: 0
