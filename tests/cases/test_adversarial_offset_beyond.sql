-- adversarial: OFFSET beyond total rows
-- setup:
CREATE TABLE t_ob (id INT);
INSERT INTO t_ob VALUES (1);
INSERT INTO t_ob VALUES (2);
-- input:
SELECT * FROM t_ob LIMIT 10 OFFSET 100;
-- expected output:
