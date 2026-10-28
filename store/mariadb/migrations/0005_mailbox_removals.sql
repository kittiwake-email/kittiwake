CREATE TABLE IF NOT EXISTS mailbox_removals (
 uuid BINARY(16) NOT NULL PRIMARY KEY,
 storage_id INT UNSIGNED NULL,
 CONSTRAINT fk_removals_storage FOREIGN KEY (storage_id) REFERENCES storage (id)
) ENGINE=InnoDB;
