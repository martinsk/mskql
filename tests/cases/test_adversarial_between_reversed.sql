-- adversarial: BETWEEN with reversed bounds (high, low)
-- setup:
CREATE TABLE t_br (v INT);
INSERT INTO t_br VALUES (5);
INSERT INTO t_br VALUES (10);
INSERT INTO t_br VALUES (15);
-- input:
SELECT v FROM t_br WHERE v BETWEEN 15 AND 5;
-- expected output:
