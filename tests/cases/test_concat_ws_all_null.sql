-- CONCAT_WS with all NULL arguments returns empty string
-- input:
SELECT CONCAT_WS(',', NULL, NULL, NULL);
-- expected output:

-- expected status: 0
