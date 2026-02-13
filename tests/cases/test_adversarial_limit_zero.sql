-- adversarial: LIMIT 0 — should return no rows
-- setup:
CREATE TABLE t_lz (id INT);
INSERT INTO t_lz VALUES (1);
INSERT INTO t_lz VALUES (2);
-- input:
SELECT * FROM t_lz LIMIT 0;
-- expected output:
