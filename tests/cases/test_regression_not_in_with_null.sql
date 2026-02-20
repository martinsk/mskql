-- BUG: NOT IN with NULL in list should return no rows (SQL three-valued logic)
-- In PostgreSQL: x NOT IN (1, NULL) = x!=1 AND x!=NULL = unknown for all x
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1), (2), (3);
-- input:
SELECT * FROM t WHERE id NOT IN (1, NULL) ORDER BY id;
-- expected output:
-- expected status: 0
