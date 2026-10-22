-- Node registry: every process registers each mode it runs and refreshes
-- last_seen every heartbeat; rows past `expires` are dead.
CREATE TABLE IF NOT EXISTS nodes (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  mode      VARCHAR(16)     NOT NULL,
  address   VARCHAR(255)    NOT NULL,
  host      VARCHAR(255)    NOT NULL DEFAULT '',
  version   VARCHAR(64)     NOT NULL DEFAULT '',
  started   DATETIME(6)     NOT NULL,
  last_seen DATETIME(6)     NOT NULL,
  expires   DATETIME(6)     NOT NULL,
  UNIQUE KEY uk_nodes_mode_address (mode, address),
  KEY ix_nodes_expires (expires)
) ENGINE=InnoDB;
