-- ON CONFLICT DO NOTHING with multiple rows: only non-conflicting rows inserted
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice'), (3, 'carol');
-- input:
INSERT INTO t1 (id, name) VALUES (1, 'dup1'), (2, 'bob'), (3, 'dup3'), (4, 'dave') ON CONFLICT DO NOTHING;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
INSERT 0 2
1|alice
2|bob
3|carol
4|dave
-- expected status: 0
