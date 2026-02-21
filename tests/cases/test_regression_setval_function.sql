-- BUG: SETVAL() function not supported
-- setup:
CREATE SEQUENCE s;
SELECT NEXTVAL('s');
-- input:
SELECT SETVAL('s', 100);
-- expected output:
100
-- expected status: 0
