-- adversarial: generate_series where start > stop (positive step) — should return empty
-- input:
SELECT * FROM generate_series(10, 1);
-- expected output:
