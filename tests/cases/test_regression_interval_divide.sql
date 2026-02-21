-- BUG: INTERVAL divided by integer returns 0 instead of scaled interval
-- input:
SELECT INTERVAL '6 hours' / 2;
-- expected output:
03:00:00
-- expected status: 0
