-- Platform-wide audit records (admin and CLI actions, records without a
-- mailbox). Per-mailbox audit stays with the mail data on its cluster.
CREATE TABLE IF NOT EXISTS audit_admin (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  time      DATETIME(6)     NOT NULL,
  account   VARCHAR(320)    NOT NULL DEFAULT '',
  admin     VARCHAR(320)    NOT NULL DEFAULT '',
  action    VARCHAR(64)     NOT NULL,
  client_ip VARCHAR(45)     NOT NULL DEFAULT '',
  record    JSON            NOT NULL,
  KEY ix_audit_admin_time (time, id)
) ENGINE=InnoDB;
