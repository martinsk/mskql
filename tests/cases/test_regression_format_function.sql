-- BUG: FORMAT() function not supported
-- input:
SELECT FORMAT('Hello %s', 'world');
-- expected output:
Hello world
-- expected status: 0
