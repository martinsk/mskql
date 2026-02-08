-- on conflict with explicit column
-- setup:
CREATE TABLE upsert2 (id INT UNIQUE, name TEXT);
INSERT INTO upsert2 (id, name) VALUES (1, 'alice'), (2, 'bob');
-- input:
INSERT INTO upsert2 (id, name) VALUES (2, 'charlie'), (3, 'dave') ON CONFLICT (id) DO NOTHING;
SELECT id, name FROM upsert2 ORDER BY id;
-- expected output:
INSERT 0 1
1|alice
2|bob
3|dave
-- expected status: 0
