-- bug: GROUP BY positional with CASE expression loses COUNT(*) value
-- setup:
CREATE TABLE gb_case (val INT);
INSERT INTO gb_case VALUES (1), (2), (3), (4), (5);
-- input:
SELECT CASE WHEN val <= 2 THEN 'low' ELSE 'high' END AS bucket, COUNT(*) AS cnt FROM gb_case GROUP BY 1 ORDER BY bucket;
-- expected output:
high|3
low|2
