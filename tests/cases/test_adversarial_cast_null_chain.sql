-- adversarial: CAST NULL through multiple types
-- setup:
CREATE TABLE t_cn (v INT);
INSERT INTO t_cn VALUES (NULL);
-- input:
SELECT CAST(CAST(v AS TEXT) AS INT) FROM t_cn;
-- expected output:

