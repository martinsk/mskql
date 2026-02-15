-- regression: CREATE TABLE AS SELECT
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1,10),(2,20),(3,30);
-- input:
CREATE TABLE t_copy AS SELECT id, val * 2 as doubled FROM t WHERE val > 10;
SELECT * FROM t_copy ORDER BY id;
-- expected output:
SELECT 2
2|40
3|60
