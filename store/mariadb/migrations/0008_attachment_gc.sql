CREATE TABLE IF NOT EXISTS attachment_gc (
 storage_id INT UNSIGNED NOT NULL,
 object_id VARBINARY(128) NOT NULL,
 token BINARY(16) NOT NULL,
 PRIMARY KEY (storage_id, object_id)
) ENGINE=InnoDB;
