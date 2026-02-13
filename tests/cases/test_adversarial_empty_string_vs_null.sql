-- adversarial: empty string vs NULL — should be distinct
-- setup:
CREATE TABLE t_esn (s TEXT);
INSERT INTO t_esn VALUES ('');
INSERT INTO t_esn VALUES (NULL);
-- input:
SELECT s IS NULL, LENGTH(s) FROM t_esn ORDER BY s IS NULL;
-- expected output:
f|0
t|
