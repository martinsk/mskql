-- bug: INSERT ON CONFLICT DO UPDATE RETURNING does not return the updated row on conflict
-- setup:
CREATE TABLE t_upsert_ret (id INT PRIMARY KEY, val INT);
INSERT INTO t_upsert_ret VALUES (1, 10);
-- input:
INSERT INTO t_upsert_ret VALUES (1, 20) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val RETURNING *;
-- expected output:
1|20
INSERT 0 1
-- expected status: 0
