-- COUNT(col) should skip NULL values, COUNT(*) should count all rows
-- setup:
CREATE TABLE nums (id INT, val INT);
INSERT INTO nums (id, val) VALUES (1, 10), (2, NULL), (3, 30);
-- input:
SELECT COUNT(val) FROM nums;
-- expected output:
2
-- expected status: 0
