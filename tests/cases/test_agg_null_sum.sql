-- SUM should skip NULL values (SQL standard: NULLs ignored in aggregates)
-- setup:
CREATE TABLE nums (id INT, val INT);
INSERT INTO nums (id, val) VALUES (1, 10), (2, NULL), (3, 30);
-- input:
SELECT SUM(val) FROM nums;
-- expected output:
40
-- expected status: 0
