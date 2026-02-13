-- adversarial: SUBSTRING starting beyond string length
-- setup:
CREATE TABLE t_sb (s TEXT);
INSERT INTO t_sb VALUES ('hi');
-- input:
SELECT SUBSTRING(s, 100, 5) FROM t_sb;
-- expected output:

