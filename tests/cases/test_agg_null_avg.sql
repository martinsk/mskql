-- AVG should skip NULL values (divide by non-null count only)
-- setup:
CREATE TABLE nums (id INT, val INT);
INSERT INTO nums (id, val) VALUES (1, 10), (2, NULL), (3, 30);
-- input:
SELECT AVG(val) FROM nums;
-- expected output:
20
-- expected status: 0
