-- bug: SUM(DISTINCT) ignores DISTINCT (returns same as SUM)
-- setup:
CREATE TABLE sum_dist (val INT);
INSERT INTO sum_dist VALUES (1), (1), (2), (3), (3);
-- input:
SELECT SUM(DISTINCT val) AS sd FROM sum_dist;
-- expected output:
6
