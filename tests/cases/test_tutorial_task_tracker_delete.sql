-- tutorial: task tracker - DELETE done tasks (task-tracker.html step 8)
-- setup:
CREATE TABLE projects (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE tasks (id SERIAL PRIMARY KEY, project_id INT NOT NULL REFERENCES projects(id), title TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'todo', priority INT DEFAULT 0, created_at DATE DEFAULT '2025-01-15');
CREATE TABLE labels (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE);
CREATE TABLE task_labels (task_id INT NOT NULL REFERENCES tasks(id), label_id INT NOT NULL REFERENCES labels(id));
INSERT INTO projects (name) VALUES ('Website Redesign'), ('Mobile App'), ('API Platform');
INSERT INTO labels (name) VALUES ('bug'), ('feature'), ('urgent'), ('backend'), ('frontend');
INSERT INTO tasks (project_id, title, status, priority, created_at) VALUES (1, 'Update landing page', 'in_progress', 2, '2025-01-10'), (1, 'Fix nav overflow', 'done', 3, '2025-01-12'), (1, 'Add dark mode', 'todo', 1, '2025-01-14'), (2, 'Login screen', 'done', 2, '2025-01-05'), (2, 'Push notifications', 'in_progress', 3, '2025-01-08'), (2, 'Offline sync', 'todo', 1, '2025-01-15'), (3, 'Rate limiting', 'todo', 3, '2025-01-11'), (3, 'OAuth2 integration', 'in_progress', 2, '2025-01-09'), (3, 'Batch endpoint', 'done', 1, '2025-01-06');
INSERT INTO task_labels (task_id, label_id) VALUES (1, 5), (2, 1), (2, 5), (2, 3), (3, 2), (4, 2), (5, 2), (5, 4), (6, 2), (6, 4), (7, 4), (7, 3), (8, 4), (9, 4);
DELETE FROM task_labels WHERE task_id IN (SELECT id FROM tasks WHERE status = 'done');
DELETE FROM tasks WHERE status = 'done';
-- input:
SELECT id, title FROM tasks ORDER BY id;
-- expected output:
1|Update landing page
3|Add dark mode
5|Push notifications
6|Offline sync
7|Rate limiting
8|OAuth2 integration
-- expected status: 0
