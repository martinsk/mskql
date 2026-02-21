-- BUG: INTERVAL with day component multiplied by integer returns 0 instead of scaled interval
-- input:
SELECT INTERVAL '1 day' * 2;
-- expected output:
2 days
-- expected status: 0
