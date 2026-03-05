-- Test vectorized LEFT/RIGHT
CREATE TABLE t_lr (s TEXT);
INSERT INTO t_lr VALUES ('hello');
INSERT INTO t_lr VALUES ('world');
INSERT INTO t_lr VALUES ('ab');

SELECT LEFT(s, 3) FROM t_lr ORDER BY s;
-- expected: ab
-- expected: hel
-- expected: wor

SELECT RIGHT(s, 3) FROM t_lr ORDER BY s;
-- expected: ab
-- expected: llo
-- expected: rld
