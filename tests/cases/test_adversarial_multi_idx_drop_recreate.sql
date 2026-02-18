-- adversarial: DROP INDEX then CREATE INDEX with different columns
-- setup:
CREATE TABLE t_amrc (a INT, b INT, c INT, val TEXT);
INSERT INTO t_amrc VALUES (1, 2, 3, 'row1');
INSERT INTO t_amrc VALUES (4, 5, 6, 'row2');
CREATE INDEX idx_amrc ON t_amrc (a, b);
DROP INDEX idx_amrc;
CREATE INDEX idx_amrc ON t_amrc (b, c);
-- input:
SELECT val FROM t_amrc WHERE b = 5 AND c = 6;
-- expected output:
row2
