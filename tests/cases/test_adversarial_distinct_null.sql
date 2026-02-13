-- adversarial: DISTINCT with NULLs — should treat NULLs as equal
-- setup:
CREATE TABLE t_dn (v INT);
INSERT INTO t_dn VALUES (1);
INSERT INTO t_dn VALUES (NULL);
INSERT INTO t_dn VALUES (1);
INSERT INTO t_dn VALUES (NULL);
INSERT INTO t_dn VALUES (2);
-- input:
SELECT DISTINCT v FROM t_dn ORDER BY v;
-- expected output:
1
2
