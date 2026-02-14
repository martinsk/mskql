-- tutorial: task tracker - ON CONFLICT DO NOTHING (task-tracker.html step 6)
-- setup:
CREATE TABLE labels (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
INSERT INTO labels (name) VALUES ('bug'), ('feature'), ('urgent'), ('backend'), ('frontend');
-- input:
INSERT INTO labels (name) VALUES ('bug') ON CONFLICT (name) DO NOTHING;
-- expected output:
INSERT 0 0
-- expected status: 0
