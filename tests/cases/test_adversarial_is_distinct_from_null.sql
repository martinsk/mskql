-- adversarial: IS DISTINCT FROM with NULLs — NULL IS DISTINCT FROM NULL should be false
-- BUG: parser doesn't support IS DISTINCT FROM in SELECT expressions,
-- only in WHERE conditions. PostgreSQL supports this everywhere.
-- setup:
CREATE TABLE t_idf (a INT, b INT);
INSERT INTO t_idf VALUES (NULL, NULL);
INSERT INTO t_idf VALUES (1, NULL);
INSERT INTO t_idf VALUES (NULL, 1);
INSERT INTO t_idf VALUES (1, 1);
-- input:
SELECT a, b, a IS DISTINCT FROM b FROM t_idf;
-- expected status: error
