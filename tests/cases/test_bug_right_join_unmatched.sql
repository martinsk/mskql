-- bug: RIGHT JOIN drops unmatched right-side rows (acts like INNER JOIN)
-- setup:
CREATE TABLE t_rj1 (id INT, name TEXT);
CREATE TABLE t_rj2 (id INT, ref_id INT, val TEXT);
INSERT INTO t_rj1 VALUES (1, 'alice'), (2, 'bob');
INSERT INTO t_rj2 VALUES (10, 1, 'x'), (20, 3, 'y'), (30, 1, 'z');
-- input:
SELECT t_rj1.name, t_rj2.val FROM t_rj1 RIGHT JOIN t_rj2 ON t_rj1.id = t_rj2.ref_id ORDER BY t_rj2.val;
-- expected output:
alice|x
|y
alice|z
