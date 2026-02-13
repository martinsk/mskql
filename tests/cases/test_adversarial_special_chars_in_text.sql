-- adversarial: special characters in text values — quotes, backslashes, newlines
-- setup:
CREATE TABLE t_sc (s TEXT);
INSERT INTO t_sc VALUES ('it''s a test');
INSERT INTO t_sc VALUES ('back\slash');
INSERT INTO t_sc VALUES ('tab	here');
-- input:
SELECT s FROM t_sc ORDER BY s;
-- expected output:
back\slash
it's a test
tab	here
