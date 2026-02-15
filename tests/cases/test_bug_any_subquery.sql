-- bug: WHERE val > ANY (SELECT ...) returns all rows instead of filtering
-- setup:
CREATE TABLE t_any (id INT, val INT);
INSERT INTO t_any VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
-- input:
SELECT id, val FROM t_any WHERE val > ANY (SELECT val FROM t_any WHERE val IN (20, 30)) ORDER BY id;
-- expected output:
3|30
4|40
5|50
