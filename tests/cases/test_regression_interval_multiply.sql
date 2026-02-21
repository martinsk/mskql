-- BUG: INTERVAL multiplied by integer returns 0 instead of scaled interval
-- input:
SELECT INTERVAL '1 hour' * 3;
-- expected output:
03:00:00
-- expected status: 0
