-- adversarial: generate_series with zero step — should error or return empty, not infinite loop
-- input:
SELECT * FROM generate_series(1, 10, 0);
-- expected status: error
