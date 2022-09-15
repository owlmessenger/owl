# Owl Messenger
<img src="./asset/owl_logo.png"/>

Owl is a fully distributed peer-to-peer messenger, built with
[INET256](https://github.com/inet256/inet256) and SQLite.

**[Documentation](./doc/00_TOC.md)**

## Why Owl?
- **Fully distributed/peer-to-peer.**
Owl doesn't need a central server for messages.  It relies on INET256 to connect to other instances.
- **No phone number or email address required.**
- **Multi-device support.** Synchronized contact and channel lists
- **Well understood formats.**
All the state for an instance is stored in one SQLite database.
- Owl thinks of distributed messaging as a purely distributed systems problem.
As much of the cryptography as possible is offloaded to the transport layer.
INET256 provides secure message passing between nodes, and that is all Owl needs.
There are no fancy cryptographic protocols anywhere in Owl.
In fact, the only cryptographic primitive Owl uses is `SHA3_256`, which is used for references in merkle data structures.

## More
Support and discussion happen in the INET256 discord
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)
