-- bug: RETURNING with expression evaluates using old value instead of new
-- setup:
CREATE TABLE t_re (id INT, val INT);
INSERT INTO t_re VALUES (1, 10);
-- input:
UPDATE t_re SET val = val * 2 WHERE id = 1 RETURNING id, val, val * 2 as doubled;
-- expected output:
1|20|40
UPDATE 1
