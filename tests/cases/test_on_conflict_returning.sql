-- INSERT ON CONFLICT DO NOTHING should not return conflicting rows via RETURNING
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'bob'), (2, 'carol') ON CONFLICT DO NOTHING RETURNING id, name;
-- expected output:
2|carol
INSERT 0 1
-- expected status: 0
