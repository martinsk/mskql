-- adversarial: window functions on single row — LEAD/LAG should return NULL
-- setup:
CREATE TABLE t_ws (id INT, val INT);
INSERT INTO t_ws VALUES (1, 100);
-- input:
SELECT id, LAG(val) OVER (ORDER BY id), LEAD(val) OVER (ORDER BY id), ROW_NUMBER() OVER (ORDER BY id) FROM t_ws;
-- expected output:
1|||1
