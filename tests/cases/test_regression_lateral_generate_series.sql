-- bug: LATERAL with generate_series function fails to parse
-- setup:
CREATE TABLE t_lateral_gs (id INT, n INT);
INSERT INTO t_lateral_gs VALUES (1, 2), (2, 3);
-- input:
SELECT t.id, gs.x FROM t_lateral_gs t, LATERAL generate_series(1, t.n) AS gs(x) ORDER BY t.id, gs.x;
-- expected output:
1|1
1|2
2|1
2|2
2|3
-- expected status: 0
