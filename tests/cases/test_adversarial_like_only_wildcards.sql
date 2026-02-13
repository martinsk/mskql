-- adversarial: LIKE with only wildcards
-- setup:
CREATE TABLE t_lw (s TEXT);
INSERT INTO t_lw VALUES ('anything');
INSERT INTO t_lw VALUES ('');
-- input:
SELECT s FROM t_lw WHERE s LIKE '%%%';
-- expected output:
anything
