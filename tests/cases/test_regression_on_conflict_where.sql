-- BUG: ON CONFLICT DO UPDATE SET ... WHERE condition is ignored
-- setup:
CREATE TABLE t (id INT PRIMARY KEY, val INT);
INSERT INTO t VALUES (1, 10);
INSERT INTO t VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val WHERE t.val > 15;
-- input:
SELECT * FROM t;
-- expected output:
1|10
-- expected status: 0
