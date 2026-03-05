-- Test vectorized CAST to/from text
CREATE TABLE t_cast (a INT, b TEXT);
INSERT INTO t_cast VALUES (42, '123');
INSERT INTO t_cast VALUES (100, '456');
INSERT INTO t_cast VALUES (-7, '789');

-- CAST(int AS TEXT)
SELECT CAST(a AS TEXT) FROM t_cast ORDER BY a;

-- CAST(text AS INT)
SELECT CAST(b AS INT) FROM t_cast ORDER BY a;

-- CAST(text AS BIGINT)
SELECT CAST(b AS BIGINT) FROM t_cast ORDER BY a;
