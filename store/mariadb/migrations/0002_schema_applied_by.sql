-- Schema version 2: record which product version applied each migration.
-- (Already part of version 1 for databases created after this change.)

ALTER TABLE schema_version ADD COLUMN IF NOT EXISTS applied_by VARCHAR(64) NOT NULL DEFAULT '';
