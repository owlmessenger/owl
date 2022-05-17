# Owl Messenger

Owl is a fully distributed peer-to-peer messenger, built with
[INET256](https://github.com/inet256/inet256) and SQLite.

## Why Owl?
- **Fully distributed/peer-to-peer.**
Owl doesn't need a central server for messages.  It relies on INET256 to connect to other instances.
- **No phone number or email address required.**
- **Multi-device support.**
- **Well understood formats.**
All the state for an instance is stored in one SQLite database.
- Owl thinks of distributed messaging as a purely distributed systems problem.
As much of the cryptography as possible is offloaded to the transport layer.
INET256 provides secure message passing between nodes, and that is all Owl needs.
There are no fancy cryptographic protocols anywhere in Owl.
In fact, the only cryptographic primitive Owl uses is `SHA3_256`, which is used for references in merkle data structures.
