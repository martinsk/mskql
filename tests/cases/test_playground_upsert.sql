-- playground example: upsert (ON CONFLICT DO UPDATE)
-- setup:
CREATE TABLE kv (key TEXT PRIMARY KEY, value INT);
INSERT INTO kv VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO kv VALUES ('b', 20) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
INSERT INTO kv VALUES ('d', 4) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
-- input:
SELECT * FROM kv ORDER BY key;
-- expected output:
a|1
b|2
c|3
d|4
-- expected status: 0
