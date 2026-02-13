-- adversarial: SUBSTRING with negative/zero positions
-- setup:
CREATE TABLE t_sub (s TEXT);
INSERT INTO t_sub VALUES ('hello world');
-- input:
SELECT SUBSTRING(s, 0, 5), SUBSTRING(s, -3, 5), SUBSTRING(s, 1, 0) FROM t_sub;
-- expected output:
hello|hello|
