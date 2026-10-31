-- Proxy keys: the public Ed25519 keys of the proxy sites that verify
-- passwords themselves (proxy.auth local) and hand clients to the
-- backends with a signed handoff token. The id is derived from the key.
CREATE TABLE IF NOT EXISTS proxy_keys (
  id         VARCHAR(16)   NOT NULL PRIMARY KEY,
  name       VARCHAR(64)   NOT NULL,
  public_key VARBINARY(32) NOT NULL,
  created    DATETIME(6)   NOT NULL,
  UNIQUE KEY uk_proxy_keys_name (name)
) ENGINE=InnoDB;
