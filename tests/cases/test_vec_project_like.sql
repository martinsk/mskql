-- Test vectorized LIKE
CREATE TABLE t_like (s TEXT);
INSERT INTO t_like VALUES ('hello');
INSERT INTO t_like VALUES ('world');
INSERT INTO t_like VALUES ('help');
INSERT INTO t_like VALUES ('heap');

-- LIKE with prefix
SELECT s, s LIKE 'he%' AS starts_he FROM t_like ORDER BY s;

-- NOT LIKE
SELECT s, s NOT LIKE '%ld' AS not_end_ld FROM t_like ORDER BY s;

-- LIKE with single char wildcard
SELECT s, s LIKE 'he_p' AS match_heap FROM t_like ORDER BY s;
