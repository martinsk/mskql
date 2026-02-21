-- BUG: E-string literals (E'...') with escape sequences not supported
-- input:
SELECT E'hello\tworld';
-- expected output:
hello	world
-- expected status: 0
