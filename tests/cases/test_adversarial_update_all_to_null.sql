-- adversarial: UPDATE all rows to NULL then aggregate
-- setup:
CREATE TABLE t_utn (id INT, val INT);
INSERT INTO t_utn VALUES (1, 10);
INSERT INTO t_utn VALUES (2, 20);
UPDATE t_utn SET val = NULL;
-- input:
SELECT SUM(val), AVG(val), MIN(val), MAX(val) FROM t_utn;
-- expected output:
|||
