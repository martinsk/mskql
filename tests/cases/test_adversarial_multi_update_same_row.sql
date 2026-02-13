-- adversarial: multiple UPDATEs to the same row
-- setup:
CREATE TABLE t_musr (id INT, val INT);
INSERT INTO t_musr VALUES (1, 0);
UPDATE t_musr SET val = val + 1 WHERE id = 1;
UPDATE t_musr SET val = val + 1 WHERE id = 1;
UPDATE t_musr SET val = val + 1 WHERE id = 1;
-- input:
SELECT val FROM t_musr WHERE id = 1;
-- expected output:
3
