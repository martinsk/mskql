-- Test vectorized IS NULL / IS NOT NULL
CREATE TABLE t_isnull (a INT, b TEXT);
INSERT INTO t_isnull VALUES (1, 'hello');
INSERT INTO t_isnull VALUES (NULL, 'world');
INSERT INTO t_isnull VALUES (3, NULL);

-- IS NULL on int column
SELECT a IS NULL FROM t_isnull ORDER BY b;
-- expected: f
-- expected: f
-- expected: t

-- IS NOT NULL on text column
SELECT b IS NOT NULL FROM t_isnull ORDER BY a;
-- expected: t
-- expected: t
-- expected: f
