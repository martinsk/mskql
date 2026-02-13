-- adversarial: table with many columns — stress column handling
-- setup:
CREATE TABLE t_mc (c1 INT, c2 INT, c3 INT, c4 INT, c5 INT, c6 INT, c7 INT, c8 INT, c9 INT, c10 INT, c11 INT, c12 INT, c13 INT, c14 INT, c15 INT, c16 INT, c17 INT, c18 INT, c19 INT, c20 INT);
INSERT INTO t_mc VALUES (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20);
-- input:
SELECT c1+c2+c3+c4+c5+c6+c7+c8+c9+c10+c11+c12+c13+c14+c15+c16+c17+c18+c19+c20 FROM t_mc;
-- expected output:
210
