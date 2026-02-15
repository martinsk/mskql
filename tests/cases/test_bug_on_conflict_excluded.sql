-- bug: ON CONFLICT DO UPDATE SET val = EXCLUDED.val uses old value instead of new
-- setup:
CREATE TABLE t_exc (id INT PRIMARY KEY, name TEXT, val INT);
INSERT INTO t_exc VALUES (1, 'old', 100);
-- input:
INSERT INTO t_exc VALUES (1, 'new', 77) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, val = EXCLUDED.val;
SELECT id, name, val FROM t_exc;
-- expected output:
INSERT 0 1
1|new|77
