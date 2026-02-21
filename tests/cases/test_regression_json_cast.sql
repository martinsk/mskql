-- BUG: Casting text to JSON/JSONB returns 0 instead of the JSON value
-- input:
SELECT '{"key": "value"}'::JSON;
-- expected output:
{"key": "value"}
-- expected status: 0
