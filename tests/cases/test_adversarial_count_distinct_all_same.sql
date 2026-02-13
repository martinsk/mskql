-- adversarial: COUNT(DISTINCT col) where all values are the same
-- setup:
CREATE TABLE t_cdas (v INT);
INSERT INTO t_cdas VALUES (42);
INSERT INTO t_cdas VALUES (42);
INSERT INTO t_cdas VALUES (42);
-- input:
SELECT COUNT(DISTINCT v) FROM t_cdas;
-- expected output:
1
