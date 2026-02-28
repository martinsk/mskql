-- HAVING with SUM aggregate expression via plan executor (not just legacy path)
-- Tests: HAVING SUM(col) > N with ORDER BY alias DESC LIMIT
-- setup:
CREATE TABLE th_sales (region TEXT, amount INT);
INSERT INTO th_sales VALUES ('east', 100), ('east', 200), ('east', 300);
INSERT INTO th_sales VALUES ('west', 50), ('west', 60);
INSERT INTO th_sales VALUES ('north', 500), ('north', 600), ('north', 700);
-- input:
SELECT region, SUM(amount) AS total, COUNT(*) FROM th_sales GROUP BY region HAVING SUM(amount) > 200 ORDER BY total DESC;
-- expected output:
north|1800|3
east|600|3
-- expected status: 0
