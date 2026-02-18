-- Test: predicate pushdown with GROUP BY + aggregates
-- Setup
CREATE TABLE pd_agg_sales (id INT PRIMARY KEY, region TEXT, product TEXT, amount INT);
INSERT INTO pd_agg_sales VALUES (1, 'east', 'widget', 100);
INSERT INTO pd_agg_sales VALUES (2, 'east', 'gadget', 200);
INSERT INTO pd_agg_sales VALUES (3, 'west', 'widget', 150);
INSERT INTO pd_agg_sales VALUES (4, 'west', 'gadget', 300);
INSERT INTO pd_agg_sales VALUES (5, 'east', 'widget', 50);

CREATE TABLE pd_agg_regions (id INT PRIMARY KEY, name TEXT, active INT);
INSERT INTO pd_agg_regions VALUES (1, 'east', 1);
INSERT INTO pd_agg_regions VALUES (2, 'west', 0);

-- Input: filter on regions.active pushable, then aggregate
SELECT r.name, SUM(s.amount)
FROM pd_agg_sales s
JOIN pd_agg_regions r ON s.region = r.name
WHERE r.active = 1
GROUP BY r.name
ORDER BY r.name;

-- Expected
-- east|350
