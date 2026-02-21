-- BUG: QUOTE_IDENT() function not supported
-- input:
SELECT QUOTE_IDENT('hello world');
-- expected output:
"hello world"
-- expected status: 0
