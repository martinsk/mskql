-- bug: COUNT(*) FILTER clause ignored with GROUP BY (returns unfiltered count)
-- setup:
CREATE TABLE agg_filter (grp TEXT, val INT);
INSERT INTO agg_filter VALUES ('a', 1), ('a', 2), ('a', 5), ('b', 3), ('b', 4), ('b', 6);
-- input:
SELECT grp, COUNT(*) FILTER (WHERE val > 3) AS cnt FROM agg_filter GROUP BY grp ORDER BY grp;
-- expected output:
a|1
b|2
