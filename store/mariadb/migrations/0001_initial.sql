-- Kittiwake metadata schema, version 1. Every statement is idempotent so a
-- migration that failed half-way can simply be re-run (MariaDB commits
-- each DDL statement by itself). Internal keys are integers; the UUIDs the
-- API shows are unique BINARY(16) columns.

CREATE TABLE IF NOT EXISTS schema_version (
  version    INT UNSIGNED NOT NULL PRIMARY KEY,
  applied_at DATETIME(6)  NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS clusters (
  id               INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  fsid             BINARY(16)   NULL,
  name             VARCHAR(64)  NOT NULL,
  mon_host         TEXT         NOT NULL,
  ceph_user        VARCHAR(64)  NOT NULL,
  ceph_key         VARCHAR(255) NOT NULL,
  pool_messages    VARCHAR(128) NOT NULL,
  pool_attachments VARCHAR(128) NOT NULL,
  pool_index       VARCHAR(128) NOT NULL,
  placement        TINYINT(1)   NOT NULL DEFAULT 1,
  created          DATETIME(6)  NOT NULL,
  updated          DATETIME(6)  NOT NULL,
  UNIQUE KEY uk_clusters_fsid (fsid),
  UNIQUE KEY uk_clusters_name (name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS domains (
  id       INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name     VARCHAR(253) NOT NULL,
  enabled  TINYINT(1)   NOT NULL DEFAULT 1,
  settings JSON         NULL,
  created  DATETIME(6)  NOT NULL,
  updated  DATETIME(6)  NOT NULL,
  UNIQUE KEY uk_domains_name (name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS mailboxes (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  uuid          BINARY(16)      NOT NULL,
  cluster_id    INT UNSIGNED    NULL,
  state         VARCHAR(16)     NOT NULL,
  state_reason  VARCHAR(255)    NOT NULL DEFAULT '',
  state_since   DATETIME(6)     NOT NULL,
  soft_bytes    BIGINT          NOT NULL DEFAULT 0,
  hard_bytes    BIGINT          NOT NULL DEFAULT 0,
  soft_messages BIGINT          NOT NULL DEFAULT 0,
  hard_messages BIGINT          NOT NULL DEFAULT 0,
  hold          JSON            NULL,
  released      DATETIME(6)     NULL,
  created       DATETIME(6)     NOT NULL,
  updated       DATETIME(6)     NOT NULL,
  UNIQUE KEY uk_mailboxes_uuid (uuid),
  KEY ix_mailboxes_cluster (cluster_id),
  CONSTRAINT fk_mailboxes_cluster FOREIGN KEY (cluster_id) REFERENCES clusters (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS accounts (
  id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  uuid       BINARY(16)      NOT NULL,
  username   VARCHAR(320)    NOT NULL,
  domain_id  INT UNSIGNED    NOT NULL,
  mailbox_id BIGINT UNSIGNED NOT NULL,
  name       VARCHAR(256)    NOT NULL DEFAULT '',
  roles      VARCHAR(64)     NOT NULL DEFAULT '',
  services   VARCHAR(255)    NOT NULL DEFAULT '',
  enabled    TINYINT(1)      NOT NULL DEFAULT 1,
  totp       JSON            NULL,
  created    DATETIME(6)     NOT NULL,
  updated    DATETIME(6)     NOT NULL,
  UNIQUE KEY uk_accounts_uuid (uuid),
  UNIQUE KEY uk_accounts_username (username),
  KEY ix_accounts_domain (domain_id, username),
  KEY ix_accounts_mailbox (mailbox_id, username),
  CONSTRAINT fk_accounts_domain FOREIGN KEY (domain_id) REFERENCES domains (id),
  CONSTRAINT fk_accounts_mailbox FOREIGN KEY (mailbox_id) REFERENCES mailboxes (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS account_passwords (
  id         CHAR(36)        NOT NULL PRIMARY KEY,
  account_id BIGINT UNSIGNED NOT NULL,
  position   INT             NOT NULL,
  hash       VARCHAR(512)    NOT NULL,
  kind       VARCHAR(16)     NOT NULL,
  comment    VARCHAR(255)    NOT NULL DEFAULT '',
  expires_at DATETIME(6)     NULL,
  created    DATETIME(6)     NOT NULL,
  KEY ix_account_passwords_account (account_id, position),
  CONSTRAINT fk_account_passwords_account FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS aliases (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  address   VARCHAR(320)    NOT NULL,
  domain_id INT UNSIGNED    NOT NULL,
  enabled   TINYINT(1)      NOT NULL DEFAULT 1,
  created   DATETIME(6)     NOT NULL,
  updated   DATETIME(6)     NOT NULL,
  UNIQUE KEY uk_aliases_address (address),
  KEY ix_aliases_domain (domain_id, address),
  CONSTRAINT fk_aliases_domain FOREIGN KEY (domain_id) REFERENCES domains (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS alias_destinations (
  alias_id   BIGINT UNSIGNED NOT NULL,
  position   INT             NOT NULL,
  mailbox_id BIGINT UNSIGNED NULL,
  address    VARCHAR(320)    NULL,
  PRIMARY KEY (alias_id, position),
  KEY ix_alias_destinations_mailbox (mailbox_id),
  CONSTRAINT fk_alias_destinations_alias FOREIGN KEY (alias_id) REFERENCES aliases (id) ON DELETE CASCADE,
  CONSTRAINT fk_alias_destinations_mailbox FOREIGN KEY (mailbox_id) REFERENCES mailboxes (id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tokens (
  id         CHAR(36)        NOT NULL PRIMARY KEY,
  hash       CHAR(64)        NOT NULL,
  account_id BIGINT UNSIGNED NULL,
  roles      VARCHAR(64)     NOT NULL DEFAULT '',
  comment    VARCHAR(255)    NOT NULL,
  expires_at DATETIME(6)     NOT NULL,
  created    DATETIME(6)     NOT NULL,
  last_used  DATETIME(6)     NULL,
  UNIQUE KEY uk_tokens_hash (hash),
  KEY ix_tokens_account (account_id),
  CONSTRAINT fk_tokens_account FOREIGN KEY (account_id) REFERENCES accounts (id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS signing_keys (
  id      TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  keyset  JSON             NOT NULL,
  version BIGINT UNSIGNED  NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sessions (
  id                CHAR(36)     NOT NULL PRIMARY KEY,
  account_uuid      BINARY(16)   NOT NULL,
  refresh_hash      CHAR(64)     NOT NULL,
  prev_refresh_hash CHAR(64)     NULL,
  roles             VARCHAR(64)  NOT NULL DEFAULT '',
  client            VARCHAR(255) NOT NULL DEFAULT '',
  created           DATETIME(6)  NOT NULL,
  last_used         DATETIME(6)  NOT NULL,
  last_ip           VARCHAR(45)  NOT NULL DEFAULT '',
  expires_at        DATETIME(6)  NOT NULL,
  KEY ix_sessions_account (account_uuid),
  KEY ix_sessions_expires (expires_at)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS totp_steps (
  account_uuid BINARY(16) NOT NULL PRIMARY KEY,
  last_step    BIGINT     NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tasks (
  id           CHAR(36)     NOT NULL PRIMARY KEY,
  type         VARCHAR(64)  NOT NULL,
  state        ENUM('pending','leased','dead') NOT NULL,
  payload      JSON         NOT NULL,
  run_after    DATETIME(6)  NOT NULL,
  attempts     INT          NOT NULL DEFAULT 0,
  last_error   TEXT         NULL,
  created      DATETIME(6)  NOT NULL,
  lease_worker VARCHAR(128) NULL,
  lease_until  DATETIME(6)  NULL,
  KEY ix_tasks_due (type, state, run_after),
  KEY ix_tasks_lease (type, state, lease_until)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS scheduler (
  name     VARCHAR(64) NOT NULL PRIMARY KEY,
  last_run DATETIME(6) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS scheduler_lease (
  id     TINYINT UNSIGNED NOT NULL PRIMARY KEY,
  holder VARCHAR(128)     NOT NULL,
  until  DATETIME(6)      NOT NULL
) ENGINE=InnoDB;
