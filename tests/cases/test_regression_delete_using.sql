-- regression: DELETE USING only removes matched rows
-- setup:
CREATE TABLE t_a (id INT, val INT);
CREATE TABLE t_b (ref_id INT);
INSERT INTO t_a VALUES (1,10),(2,20),(3,30);
INSERT INTO t_b VALUES (1),(3);
-- input:
DELETE FROM t_a USING t_b WHERE t_a.id = t_b.ref_id;
SELECT id, val FROM t_a ORDER BY id;
-- expected output:
DELETE 2
2|20
