-- Namespaces: the unit of tenant isolation. A namespace owns storage
-- (pool sets with their own credentials), domains and, through them,
-- mailboxes and accounts; admin tokens may be bound to one; daemons,
-- tasks and scheduler leases are partitioned by it. The default
-- namespace (id 1) always exists. This schema assumes empty tables: the
-- namespace columns default to the default namespace, nothing is
-- backfilled beyond that.
CREATE TABLE IF NOT EXISTS namespaces (
  id       INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name     VARCHAR(63)  NOT NULL,
  enabled  TINYINT(1)   NOT NULL DEFAULT 1,
  settings JSON         NULL,
  created  DATETIME(6)  NOT NULL,
  updated  DATETIME(6)  NOT NULL,
  UNIQUE KEY uk_namespaces_name (name)
) ENGINE=InnoDB;
INSERT IGNORE INTO namespaces (id, name, enabled, settings, created, updated) VALUES (1, 'default', 1, NULL, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6));

-- Storage: one namespace each; several storages may share a cluster
-- (fsid) but never a pool.
ALTER TABLE storage ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER fsid;
ALTER TABLE storage DROP INDEX IF EXISTS uk_storage_fsid;
ALTER TABLE storage ADD UNIQUE KEY IF NOT EXISTS uk_storage_fsid_messages (fsid, pool_messages);
ALTER TABLE storage ADD UNIQUE KEY IF NOT EXISTS uk_storage_fsid_attachments (fsid, pool_attachments);
ALTER TABLE storage ADD UNIQUE KEY IF NOT EXISTS uk_storage_fsid_index (fsid, pool_index);
ALTER TABLE storage ADD INDEX IF NOT EXISTS ix_storage_namespace (namespace_id);
ALTER TABLE storage ADD CONSTRAINT fk_storage_namespace FOREIGN KEY IF NOT EXISTS (namespace_id) REFERENCES namespaces (id);

ALTER TABLE domains ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER name;
ALTER TABLE domains ADD INDEX IF NOT EXISTS ix_domains_namespace (namespace_id, name);
ALTER TABLE domains ADD CONSTRAINT fk_domains_namespace FOREIGN KEY IF NOT EXISTS (namespace_id) REFERENCES namespaces (id);

ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER uuid;
ALTER TABLE mailboxes ADD INDEX IF NOT EXISTS ix_mailboxes_namespace (namespace_id, id);
ALTER TABLE mailboxes ADD CONSTRAINT fk_mailboxes_namespace FOREIGN KEY IF NOT EXISTS (namespace_id) REFERENCES namespaces (id);

-- Admin tokens: NULL = the whole platform, else a tenant admin of that namespace.
ALTER TABLE tokens ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NULL AFTER account_id;
ALTER TABLE tokens ADD CONSTRAINT fk_tokens_namespace FOREIGN KEY IF NOT EXISTS (namespace_id) REFERENCES namespaces (id);

-- Nodes record the namespaces they serve ('' = all).
ALTER TABLE nodes ADD COLUMN IF NOT EXISTS namespaces VARCHAR(2048) NOT NULL DEFAULT '' AFTER version;

-- Platform audit records carry the namespace of what was acted upon
-- ('' for platform-level actions) so a tenant admin reads only its own.
ALTER TABLE audit_admin ADD COLUMN IF NOT EXISTS namespace VARCHAR(63) NOT NULL DEFAULT '' AFTER admin;
ALTER TABLE audit_admin ADD INDEX IF NOT EXISTS ix_audit_admin_namespace (namespace, time, id);

-- Tasks are partitioned by namespace so that workers and backup nodes
-- serving a subset dequeue only theirs.
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER type;
ALTER TABLE tasks ADD INDEX IF NOT EXISTS ix_tasks_namespace_due (namespace_id, type, state, run_after);

-- Scheduler state (leader leases, last-run times, maintenance scans) is
-- kept per namespace: each namespace has its own leader among the nodes
-- that serve it.
ALTER TABLE scheduler ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1;
ALTER TABLE scheduler DROP PRIMARY KEY, ADD PRIMARY KEY (name, namespace_id);
ALTER TABLE scheduler_lease ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1;
ALTER TABLE scheduler_lease DROP PRIMARY KEY, ADD PRIMARY KEY (id, namespace_id);
ALTER TABLE maintenance_scans ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1;
ALTER TABLE maintenance_scans DROP PRIMARY KEY, ADD PRIMARY KEY (name, namespace_id);

-- Backup catalog rows carry the namespace so expiry runs per namespace
-- against that namespace's target, also for mailboxes removed since.
ALTER TABLE backup_runs ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER mailbox;
ALTER TABLE backup_runs ADD INDEX IF NOT EXISTS ix_backup_runs_namespace_expires (namespace_id, expires);
ALTER TABLE backup_blobs ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER name;
ALTER TABLE backup_blobs ADD INDEX IF NOT EXISTS ix_backup_blobs_namespace (namespace_id, created);
ALTER TABLE backup_jobs ADD COLUMN IF NOT EXISTS namespace_id INT UNSIGNED NOT NULL DEFAULT 1 AFTER mailbox;
ALTER TABLE backup_jobs ADD INDEX IF NOT EXISTS ix_backup_jobs_namespace_expires (namespace_id, expires);
