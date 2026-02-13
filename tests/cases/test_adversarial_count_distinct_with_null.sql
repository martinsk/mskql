-- adversarial: COUNT(DISTINCT col) with NULLs — NULLs should not be counted
-- setup:
CREATE TABLE t_cdwn (v INT);
INSERT INTO t_cdwn VALUES (1);
INSERT INTO t_cdwn VALUES (NULL);
INSERT INTO t_cdwn VALUES (2);
INSERT INTO t_cdwn VALUES (NULL);
INSERT INTO t_cdwn VALUES (1);
-- input:
SELECT COUNT(DISTINCT v) FROM t_cdwn;
-- expected output:
2
