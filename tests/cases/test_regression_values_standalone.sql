-- BUG: VALUES as a standalone query not supported
-- input:
VALUES (1, 'a'), (2, 'b');
-- expected output:
1|a
2|b
-- expected status: 0
