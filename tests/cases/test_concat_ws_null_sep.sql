-- CONCAT_WS with NULL separator returns NULL
-- input:
SELECT CONCAT_WS(NULL, 'a', 'b');
-- expected output:

-- expected status: 0
