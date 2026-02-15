-- regression: ON CONFLICT DO UPDATE SET uses EXCLUDED values
-- setup:
CREATE TABLE t (id INT PRIMARY KEY, val INT);
INSERT INTO t VALUES (1, 100);
-- input:
INSERT INTO t VALUES (1, 77) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SELECT id, val FROM t;
-- expected output:
INSERT 0 1
1|77
