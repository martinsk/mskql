-- adversarial: LIKE with _ on empty string
-- setup:
CREATE TABLE t_lu (s TEXT);
INSERT INTO t_lu VALUES ('');
INSERT INTO t_lu VALUES ('a');
INSERT INTO t_lu VALUES ('ab');
-- input:
SELECT s FROM t_lu WHERE s LIKE '_';
-- expected output:
a
