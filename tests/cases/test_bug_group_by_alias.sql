-- bug: GROUP BY does not resolve column aliases from SELECT list
-- setup:
CREATE TABLE gb_alias (category TEXT, val INT);
INSERT INTO gb_alias VALUES ('a', 1), ('a', 2), ('b', 3);
-- input:
SELECT category AS cat, SUM(val) AS total FROM gb_alias GROUP BY cat ORDER BY cat;
-- expected output:
a|3
b|3
