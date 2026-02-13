-- adversarial: INSERT ON CONFLICT where every row conflicts
-- setup:
CREATE TABLE t_oc (id INT PRIMARY KEY, val INT);
INSERT INTO t_oc VALUES (1, 10);
INSERT INTO t_oc VALUES (2, 20);
INSERT INTO t_oc VALUES (1, 100) ON CONFLICT (id) DO UPDATE SET val = 100;
INSERT INTO t_oc VALUES (2, 200) ON CONFLICT (id) DO UPDATE SET val = 200;
-- input:
SELECT id, val FROM t_oc ORDER BY id;
-- expected output:
1|100
2|200
