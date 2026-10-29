-- Mailbox backups: per-mailbox settings and schedule state, the run
-- catalog, deduplicated blob bookkeeping and restore/export jobs. The
-- backup data itself lives on the backup target; these tables let the
-- backup mode find due work and known objects without listing the bucket.
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_interval INT UNSIGNED NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_retention INT UNSIGNED NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_disabled TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_every INT UNSIGNED NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_due DATETIME(6) NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_maintain_due DATETIME(6) NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_run VARCHAR(40) NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_modseq BIGINT UNSIGNED NOT NULL DEFAULT 0;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_at DATETIME(6) NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_status VARCHAR(16) NOT NULL DEFAULT '';
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_error VARCHAR(255) NOT NULL DEFAULT '';
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_full TINYINT(1) NOT NULL DEFAULT 0;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_verified DATETIME(6) NULL;
ALTER TABLE mailboxes ADD COLUMN IF NOT EXISTS backup_verify_error VARCHAR(255) NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS ix_mailboxes_backup_due ON mailboxes (backup_due, id);
CREATE INDEX IF NOT EXISTS ix_mailboxes_backup_maintain ON mailboxes (backup_maintain_due, id);
CREATE TABLE IF NOT EXISTS backup_runs (
  mailbox      BINARY(16)      NOT NULL,
  id           VARCHAR(40)     NOT NULL,
  started      DATETIME(6)     NOT NULL,
  finished     DATETIME(6)     NOT NULL,
  modseq       BIGINT UNSIGNED NOT NULL,
  fuzzy        TINYINT(1)      NOT NULL DEFAULT 0,
  full         TINYINT(1)      NOT NULL DEFAULT 0,
  manifest_key VARCHAR(255)    NOT NULL,
  packs        JSON            NULL,
  bytes        BIGINT          NOT NULL DEFAULT 0,
  messages     INT             NOT NULL DEFAULT 0,
  new_messages INT             NOT NULL DEFAULT 0,
  expires      DATETIME(6)     NULL,
  PRIMARY KEY (mailbox, id),
  KEY ix_backup_runs_expires (expires)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS backup_blobs (
  name    CHAR(64)    NOT NULL PRIMARY KEY,
  size    BIGINT      NOT NULL,
  sealed  BIGINT      NOT NULL,
  created DATETIME(6) NOT NULL
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS backup_blob_refs (
  name    CHAR(64)   NOT NULL,
  mailbox BINARY(16) NOT NULL,
  PRIMARY KEY (name, mailbox),
  KEY ix_backup_blob_refs_mailbox (mailbox)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS backup_jobs (
  id           VARCHAR(36)  NOT NULL PRIMARY KEY,
  kind         VARCHAR(16)  NOT NULL,
  mailbox      BINARY(16)   NOT NULL,
  run_id       VARCHAR(40)  NOT NULL DEFAULT '',
  target       BINARY(16)   NULL,
  message      BINARY(16)   NULL,
  object_key   VARCHAR(255) NOT NULL DEFAULT '',
  url          VARCHAR(2048) NOT NULL DEFAULT '',
  detail       JSON         NULL,
  bytes        BIGINT       NOT NULL DEFAULT 0,
  messages     INT          NOT NULL DEFAULT 0,
  state        VARCHAR(16)  NOT NULL,
  error        VARCHAR(255) NOT NULL DEFAULT '',
  requested_by VARCHAR(255) NOT NULL DEFAULT '',
  created      DATETIME(6)  NOT NULL,
  finished     DATETIME(6)  NULL,
  expires      DATETIME(6)  NULL,
  KEY ix_backup_jobs_mailbox (mailbox, created),
  KEY ix_backup_jobs_expires (expires)
) ENGINE=InnoDB;
