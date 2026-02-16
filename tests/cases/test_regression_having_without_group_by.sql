-- bug: HAVING without GROUP BY is ignored (should filter the single aggregate row)
-- setup:
CREATE TABLE having_no_gb (val INT);
INSERT INTO having_no_gb VALUES (1), (2), (3);
-- input:
SELECT SUM(val) FROM having_no_gb HAVING SUM(val) > 10;
SELECT SUM(val) FROM having_no_gb HAVING SUM(val) > 1;
-- expected output:
6
