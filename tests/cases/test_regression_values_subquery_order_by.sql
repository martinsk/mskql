-- Bug: SELECT with ORDER BY from a VALUES subquery with single-column alias returns error
-- SELECT v FROM (VALUES (3),(1),(2)) AS t(v) ORDER BY v gives "column v does not exist"
-- The alias works for multi-column VALUES but not single-column
-- setup:
-- input:
SELECT v FROM (VALUES (3),(1),(2)) AS t(v) ORDER BY v;
-- expected output:
1
2
3
-- expected status: 0
