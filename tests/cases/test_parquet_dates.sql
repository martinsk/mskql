-- parquet foreign table with date and timestamp columns
-- setup:
CREATE FOREIGN TABLE dates OPTIONS (FILENAME '@@FIXTURES@@/dates.parquet');
-- input:
SELECT * FROM dates;
-- expected output:
1|2024-01-01|2024-01-01 10:00:00
2|2024-01-02|2024-01-01 11:00:00
3|2024-01-03|2024-01-01 12:00:00
4|2024-01-04|2024-01-01 13:00:00
5|2024-01-05|2024-01-01 14:00:00
-- expected status: 0
