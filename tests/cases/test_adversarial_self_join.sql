-- adversarial: self-join — same table on both sides
-- setup:
CREATE TABLE t_sj (id INT, parent_id INT, name TEXT);
INSERT INTO t_sj VALUES (1, NULL, 'root');
INSERT INTO t_sj VALUES (2, 1, 'child1');
INSERT INTO t_sj VALUES (3, 1, 'child2');
-- input:
SELECT c.name, p.name FROM t_sj c JOIN t_sj p ON c.parent_id = p.id ORDER BY c.name;
-- expected output:
child1|root
child2|root
