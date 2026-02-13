-- adversarial: UPDATE referencing non-existent column
-- setup:
CREATE TABLE t_unc (a INT);
INSERT INTO t_unc VALUES (1);
-- input:
UPDATE t_unc SET nonexistent = 42;
-- expected status: error
