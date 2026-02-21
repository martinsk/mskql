-- BUG: LATERAL subquery crashes the server
-- input:
SELECT t.id, s.n FROM (SELECT 1 AS id) t, LATERAL (SELECT t.id * 10 AS n) s;
-- expected output:
1|10
-- expected status: 0
