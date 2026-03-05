-- Test vectorized COALESCE(col, col)
CREATE TABLE t_cc (a INT, b INT, c TEXT, d TEXT);
INSERT INTO t_cc VALUES (1, 10, 'hello', 'world');
INSERT INTO t_cc VALUES (NULL, 20, NULL, 'backup');
INSERT INTO t_cc VALUES (3, NULL, 'primary', NULL);

-- COALESCE(col, col) for INT
SELECT COALESCE(a, b) FROM t_cc ORDER BY b;
-- expected: 1
-- expected: 20
-- expected: 3

-- COALESCE(col, col) for TEXT
SELECT COALESCE(c, d) FROM t_cc ORDER BY b;
-- expected: hello
-- expected: backup
-- expected: primary
