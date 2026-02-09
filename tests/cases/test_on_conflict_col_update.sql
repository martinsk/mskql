-- ON CONFLICT with specific column: second insert should be skipped
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT, score INT);
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 90), (2, 'bob', 80);
-- input:
INSERT INTO t1 (id, name, score) VALUES (1, 'charlie', 70) ON CONFLICT (id) DO NOTHING;
SELECT id, name, score FROM t1 ORDER BY id;
-- expected output:
INSERT 0 0
1|alice|90
2|bob|80
-- expected status: 0
