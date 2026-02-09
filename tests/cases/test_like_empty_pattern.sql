-- LIKE with percent-only pattern should match everything
-- setup:
CREATE TABLE words (id INT, word TEXT);
INSERT INTO words (id, word) VALUES (1, 'hello'), (2, ''), (3, 'world');
-- input:
SELECT word FROM words WHERE word LIKE '%' ORDER BY id;
-- expected output:
hello

world
-- expected status: 0
