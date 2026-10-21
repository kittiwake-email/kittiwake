# My New Mail Platform
This repository contains a new mailplatform which has not been assigned a name yet.

The purpose of this project is to build a new massively scalable and modern mail platform to mainly replace Dovecot. The purpose is not to replace Postfix and become an MTA, it has to work together.

# Features

## Stateless and scalable
The daemons who run this platform should be stateless, allowing them to scale out. They need to use persistent storage (Ceph RADOS) for storing e-mail messages and probably something like Redis Sentinel for communication between daemons.

More servers with more daemons means the platform can scale.

## Language
The daemon should be written in Go

## Modular and testing
The code should be modular and each module should contain extensive testing so all these tests can be validated.

## Containers
The daemon should be able to run inside containers, but not mandatory

## Protocols
Inbound e-mails should be delivered using LMTP, this means that a MTA like Postfix will receive and filter the message from the internet before delivering the actual message to this daemon.

These protocols should be supported:

- IMAP
- POP3
- Rest API
- JMAP as an option

IMAP and POP3 should work, but are not the primary target. Each mailbox should be able to be queried using a full Rest API, this API would then be used to build other looks like a modern Webmail and probably mobile app. POP3 and IMAP are there for legacy clients.

## TLS
TLS should not be handled by this daemon, a proxy like HAProxy will do this.

## IPv6-first
It should be assumed that this  will operate in a IPv6-only environment and IPv4 is a nice to have, not mandatory

## Ceph RADOS
All e-mail messages should be stored in Ceph RADOS Objects, not S3/RGW, but pure RADOS. Attachments should be stored as different objects.

E-Mail messages should be parsed and stored as JSON data in these RADOS objects, but the original headers (like received through SMTP) should also be stored in the same JSON, but unparsed to have a proper trace

Messages and attachments should be stored in different RADOS pools.

### Parsing of messages
Raw e-mail messages should be parsed into a JSON file:

- Headers as JSON array
- Body as HTML and plain text in the JSON
- Attachments extracted and stored as RADOS object

### Attachments
Use dedup for attachments by hashing them and thus allowing to refer to the same attachment from different e-mails.

# Mailboxes, Accounts and Domains
The system knows mailboxes. These have a UUID. A mailbox itself is not something.

Accounts can be created and they are linked to a mailbox. Multiple accounts can point to a mailbox allowing, this allows for multiple credentials having access to a mailbox. The account consists of a username (usually the e-mail address) and potentially multiple passwords. This will allow for application passwords

## Auth database in SQL
The mailboxes, accounts and their metadata should be stored in a MySQL/MariaDB environment. The platform should not worry about HA, this is for the DBA to maybe use MariaDB Galera. The system should only read from SQL, never write.

## SQL queries
The admin should be able to provide its own SQL queries on how to work with the platform, this allows them to have their own SQL schema, similar to Dovecot.

The basic schema should already work.

Admins should also be allowed to insert SQL entries and the platform should pick this up.

## LTMP delivery
When a messages is delivered (assumped by Postfix!) to the platform it will query the database to see if an account which that e-mail address exists and will then deliver the message to the mailbox.

# Daemon
## Configuration
The daemon should take single YAML file as configuration:

- Ceph cluster connection information
  - Monitors
  - Credentials
  - RADOS pools
- SQL information
  - Host
  - Credentials
  - SQL queries
    - Variables should be allowed in the queries like username (when logging in), domain name, etc
- Redis information (if needed)


# Inspiration
When building this, for inspiration look at:

- Dovecot
  - Exists for years, has a lot of proper e-mail handling code
- WildDuck
 - https://docs.wildduck.email/
 - Has a full API
- Postfix
  - Mainly the LMTP part
