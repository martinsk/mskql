-- ON CONFLICT DO UPDATE with expression in SET
-- setup:
CREATE TABLE t1 (id INT PRIMARY KEY, count INT);
INSERT INTO t1 VALUES (1, 10);
-- input:
INSERT INTO t1 VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET count = t1.count + 1;
SELECT id, count FROM t1;
-- expected output:
INSERT 0 1
1|11
-- expected status: 0
