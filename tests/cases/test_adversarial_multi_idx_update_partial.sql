-- adversarial: UPDATE only one column of a 2-column index
-- setup:
CREATE TABLE t_amup (a INT, b INT, val TEXT);
CREATE INDEX idx_amup ON t_amup (a, b);
INSERT INTO t_amup VALUES (1, 10, 'orig');
INSERT INTO t_amup VALUES (2, 20, 'other');
-- input:
UPDATE t_amup SET a = 3 WHERE b = 10;
SELECT val FROM t_amup WHERE a = 3 AND b = 10;
-- expected output:
UPDATE 1
orig
