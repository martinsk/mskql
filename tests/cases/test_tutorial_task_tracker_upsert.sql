-- tutorial: task tracker - upsert priority (task-tracker.html)
-- setup:
CREATE TABLE projects (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE tasks (id SERIAL PRIMARY KEY, project_id INT NOT NULL REFERENCES projects(id), title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'todo', priority INT DEFAULT 0, created_at DATE DEFAULT '2025-01-15');
INSERT INTO projects (name) VALUES ('API Platform');
INSERT INTO tasks (project_id, title, status, priority) VALUES (1, 'Rate limiting', 'todo', 3);
INSERT INTO tasks (id, project_id, title, status, priority) VALUES (1, 1, 'Rate limiting', 'todo', 5) ON CONFLICT (id) DO UPDATE SET priority = 5;
-- input:
SELECT id, title, priority FROM tasks WHERE id = 1;
-- expected output:
1|Rate limiting|5
-- expected status: 0
