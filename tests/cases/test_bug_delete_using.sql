-- bug: DELETE ... USING deletes all rows instead of only matched rows
-- setup:
CREATE TABLE t_du1 (id INT, val INT);
CREATE TABLE t_du2 (ref_id INT);
INSERT INTO t_du1 VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO t_du2 VALUES (2), (4);
-- input:
DELETE FROM t_du1 USING t_du2 WHERE t_du1.id = t_du2.ref_id;
SELECT id, val FROM t_du1 ORDER BY id;
-- expected output:
DELETE 2
1|10
3|30
