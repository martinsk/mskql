-- ON CONFLICT DO NOTHING with duplicate key
-- setup:
CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob');
-- input:
INSERT INTO t1 VALUES (1, 'charlie') ON CONFLICT (id) DO NOTHING;
SELECT id, name FROM t1 ORDER BY id;
-- expected output:
INSERT 0 0
1|alice
2|bob
-- expected status: 0
