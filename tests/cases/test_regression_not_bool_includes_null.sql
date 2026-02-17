-- NOT flag should exclude NULL rows (NOT NULL is NULL, not true)
-- setup:
CREATE TABLE t (id INT, flag BOOLEAN);
INSERT INTO t VALUES (1, true), (2, false), (3, NULL);
-- input:
SELECT * FROM t WHERE NOT flag ORDER BY id;
-- expected output:
2|f
-- expected status: 0
