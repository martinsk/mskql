-- adversarial: CAST non-numeric text to integer should not crash
-- setup:
CREATE TABLE t_cast (s TEXT);
INSERT INTO t_cast VALUES ('not_a_number');
INSERT INTO t_cast VALUES ('');
INSERT INTO t_cast VALUES ('12abc');
-- input:
SELECT s::INT FROM t_cast;
-- expected output:
0
0
12
