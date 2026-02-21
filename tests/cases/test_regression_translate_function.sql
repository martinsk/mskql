-- BUG: TRANSLATE() function not supported
-- input:
SELECT TRANSLATE('hello', 'helo', 'HELO');
-- expected output:
HELLO
-- expected status: 0
