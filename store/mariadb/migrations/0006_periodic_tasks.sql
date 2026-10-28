ALTER TABLE tasks ADD COLUMN IF NOT EXISTS periodic_key BINARY(32) NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_tasks_periodic ON tasks (periodic_key);
