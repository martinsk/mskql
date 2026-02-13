-- adversarial: INSERT with too many values — should error, not crash
-- setup:
CREATE TABLE t_itm (a INT);
-- input:
INSERT INTO t_itm VALUES (1, 2, 3, 4, 5);
-- expected status: error
