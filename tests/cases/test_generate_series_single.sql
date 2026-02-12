-- generate_series with start == stop returns single row
-- input:
SELECT * FROM generate_series(42, 42);
-- expected output:
42
-- expected status: 0
