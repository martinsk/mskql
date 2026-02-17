-- adversarial: CAST non-numeric text to integer should error
-- setup:
CREATE TABLE t_cast (s TEXT);
INSERT INTO t_cast VALUES ('not_a_number');
INSERT INTO t_cast VALUES ('');
INSERT INTO t_cast VALUES ('12abc');
-- input:
SELECT s::INT FROM t_cast;
-- expected output:
ERROR:  invalid input syntax for type integer: "not_a_number"
