-- BUG: INTERVAL '2 days' - INTERVAL '12 hours' should normalize to '1 day 12:00:00'
-- input:
SELECT INTERVAL '2 days' - INTERVAL '12 hours';
-- expected output:
1 day 12:00:00
-- expected status: 0
