-- REGRESSION: CTE with DELETE ... RETURNING not supported (errors with 'table not found')
-- setup:
CREATE TABLE t (id INT, val INT);
INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);
-- input:
WITH deleted AS (DELETE FROM t WHERE id = 2 RETURNING *) SELECT * FROM deleted;
-- expected output:
2|20
-- expected status: 0
