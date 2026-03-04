-- generate_series spanning multiple full blocks (tests full-block fast path)
-- setup:
-- input:
SELECT MIN(generate_series), MAX(generate_series), COUNT(*) FROM generate_series(1, 3000);
-- expected output:
1|3000|3000
