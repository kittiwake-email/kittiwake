-- Passwords are ordered by creation, not by a stored position. The new
-- index goes first so the foreign key on account_id always has one.
ALTER TABLE account_passwords ADD INDEX IF NOT EXISTS ix_account_passwords_created (account_id, created, id);
ALTER TABLE account_passwords DROP INDEX IF EXISTS ix_account_passwords_account;
ALTER TABLE account_passwords DROP COLUMN IF EXISTS position;
