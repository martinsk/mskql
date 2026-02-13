-- adversarial: INSERT with wrong number of values — should error, not crash
-- setup:
CREATE TABLE t_iwc (a INT, b INT, c INT);
-- input:
INSERT INTO t_iwc VALUES (1, 2);
-- expected status: error
