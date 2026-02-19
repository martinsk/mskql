-- bug: FETCH FIRST N ROWS ONLY syntax not supported (SQL standard alternative to LIMIT)
-- setup:
CREATE TABLE t_fetch (id INT);
INSERT INTO t_fetch SELECT * FROM generate_series(1, 10);
-- input:
SELECT * FROM t_fetch ORDER BY id FETCH FIRST 3 ROWS ONLY;
-- expected output:
1
2
3
-- expected status: 0
