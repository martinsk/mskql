-- BUG: EXTRACT(EPOCH FROM timestamp) returns scientific notation instead of integer
-- input:
SELECT EXTRACT(EPOCH FROM '2024-01-01 00:00:00'::TIMESTAMP);
-- expected output:
1704067200
-- expected status: 0
