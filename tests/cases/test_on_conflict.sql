-- on conflict do nothing
-- setup:
CREATE TABLE upsert_t (id INT UNIQUE, name TEXT);
INSERT INTO upsert_t (id, name) VALUES (1, 'alice');
-- input:
INSERT INTO upsert_t (id, name) VALUES (1, 'bob') ON CONFLICT DO NOTHING;
SELECT id, name FROM upsert_t;
-- expected output:
INSERT 0 0
1|alice
-- expected status: 0
