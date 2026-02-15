-- regression: RIGHT JOIN includes unmatched right rows
-- setup:
CREATE TABLE t_a (id INT, name TEXT);
CREATE TABLE t_b (ref_id INT, val TEXT);
INSERT INTO t_a VALUES (1,'alice');
INSERT INTO t_b VALUES (1,'x'),(3,'y');
-- input:
SELECT t_a.name, t_b.val FROM t_a RIGHT JOIN t_b ON t_a.id = t_b.ref_id ORDER BY t_b.val;
-- expected output:
alice|x
|y
