-- BUG: Feb 30 is not a valid date, should error not silently become Mar 1
-- input:
SELECT '2024-02-30'::DATE;
-- expected output:
ERROR:  date/time field value out of range: "2024-02-30"
-- expected status: 0
