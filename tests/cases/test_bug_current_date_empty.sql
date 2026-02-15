-- bug: CURRENT_DATE returns empty instead of today's date
-- setup:
CREATE TABLE t_cd (id INT);
INSERT INTO t_cd VALUES (1);
-- input:
SELECT CURRENT_DATE IS NOT NULL FROM t_cd;
-- expected output:
t
