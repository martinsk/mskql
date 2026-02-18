-- adversarial: CREATE INDEX on empty table, then INSERT
-- setup:
CREATE TABLE t_amet (a INT, b INT, val TEXT);
CREATE INDEX idx_amet ON t_amet (a, b);
INSERT INTO t_amet VALUES (5, 6, 'added');
-- input:
SELECT val FROM t_amet WHERE a = 5 AND b = 6;
-- expected output:
added
