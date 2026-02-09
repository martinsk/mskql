-- MIN/MAX should skip NULL values
-- setup:
CREATE TABLE nums (id INT, val INT);
INSERT INTO nums (id, val) VALUES (1, NULL), (2, 50), (3, 20);
-- input:
SELECT MIN(val), MAX(val) FROM nums;
-- expected output:
20|50
-- expected status: 0
