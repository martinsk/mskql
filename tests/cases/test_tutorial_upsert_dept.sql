-- tutorial: upsert department (index.html step 6)
-- setup:
CREATE TABLE departments (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
INSERT INTO departments (name) VALUES ('Engineering'), ('Design'), ('Sales');
INSERT INTO departments (id, name) VALUES (2, 'Product Design') ON CONFLICT (id) DO UPDATE SET name = 'Product Design';
-- input:
SELECT * FROM departments ORDER BY id;
-- expected output:
1|Engineering
2|Product Design
3|Sales
-- expected status: 0
