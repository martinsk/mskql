-- adversarial: EXISTS with empty subquery
-- setup:
CREATE TABLE t_ee1 (id INT);
CREATE TABLE t_ee2 (id INT);
INSERT INTO t_ee1 VALUES (1);
-- input:
SELECT id FROM t_ee1 WHERE EXISTS (SELECT 1 FROM t_ee2);
-- expected output:
