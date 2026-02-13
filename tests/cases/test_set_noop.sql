-- SET statements are silently accepted
-- setup:
SET client_encoding TO 'UTF8'
SET search_path TO public
-- input:
SELECT 1
-- expected output:
1
