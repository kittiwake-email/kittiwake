-- Schema version 3: aliases by destination address, for sender identities
-- (aliases that reach a mailbox through an address destination).
-- (Already part of version 1 for databases created after this change.)

CREATE INDEX IF NOT EXISTS ix_alias_destinations_address ON alias_destinations (address);
