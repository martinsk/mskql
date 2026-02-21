-- BUG: EXTRACT(DAYS FROM interval) with plural DAYS returns 0 instead of correct value
-- input:
SELECT EXTRACT(DAYS FROM INTERVAL '5 days 3 hours');
-- expected output:
5
-- expected status: 0
