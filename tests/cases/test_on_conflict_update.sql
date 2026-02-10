-- ON CONFLICT DO UPDATE (upsert) should update the conflicting row
-- setup:
CREATE TABLE t1 (id INT UNIQUE, name TEXT, score INT);
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 80);
-- input:
INSERT INTO t1 (id, name, score) VALUES (1, 'alice', 100) ON CONFLICT (id) DO UPDATE SET score = 100;
SELECT id, name, score FROM t1;
-- expected output:
INSERT 0 1
1|alice|100
-- expected status: 0
