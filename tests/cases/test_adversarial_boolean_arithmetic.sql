-- adversarial: boolean in arithmetic context — type confusion
-- setup:
CREATE TABLE t_ba (flag BOOLEAN, val INT);
INSERT INTO t_ba VALUES (true, 10);
INSERT INTO t_ba VALUES (false, 20);
-- input:
SELECT flag::INT, flag::INT + val FROM t_ba;
-- expected output:
1|11
0|20
