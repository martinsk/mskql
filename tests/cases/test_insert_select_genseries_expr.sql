-- regression: INSERT-SELECT with generate_series expression columns must not be null
-- setup:
CREATE TABLE t_gse (id INT, grp INT, val INT);
INSERT INTO t_gse SELECT n, n % 10, (n * 7) % 100 FROM generate_series(1, 5) AS g(n);
-- input:
SELECT id, grp, val FROM t_gse ORDER BY id;
-- expected output:
1|1|7
2|2|14
3|3|21
4|4|28
5|5|35
