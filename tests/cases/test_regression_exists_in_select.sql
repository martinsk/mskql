-- regression: EXISTS in SELECT list evaluates per-row
-- setup:
CREATE TABLE t_a (id INT);
CREATE TABLE t_b (ref_id INT);
INSERT INTO t_a VALUES (1),(2),(3);
INSERT INTO t_b VALUES (1),(3);
-- input:
SELECT id, EXISTS (SELECT 1 FROM t_b WHERE t_b.ref_id = t_a.id) FROM t_a ORDER BY id;
-- expected output:
1|t
2|f
3|t
