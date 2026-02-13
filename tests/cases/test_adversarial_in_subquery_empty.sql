-- adversarial: IN with subquery that returns no rows
-- setup:
CREATE TABLE t_ise1 (id INT);
CREATE TABLE t_ise2 (id INT);
INSERT INTO t_ise1 VALUES (1);
INSERT INTO t_ise1 VALUES (2);
-- input:
SELECT id FROM t_ise1 WHERE id IN (SELECT id FROM t_ise2);
-- expected output:
