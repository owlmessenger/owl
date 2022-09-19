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
All the state for an instance is stored in one SQLite database
JSON is used whereever possible.
- Owl thinks of distributed messaging as a purely distributed systems problem.
As much of the cryptography as possible is offloaded to the transport layer.
INET256 provides secure message passing between nodes, and that is all Owl needs.
There are no fancy cryptographic protocols anywhere in Owl.

## Goals
- Stable upward facing API, so that GUIs and other client applications can depend on it.
- Slack style channels with threads, reactions, and replies.
- Synchronized channel and contact lists.
- Shared channel directories analagous to Slack or Discord servers.
- Easy to export your data, you can always dump contacts and message history to JSON files.
- Avoid cryptography. Let INET256 take care of that.  So far it looks like Owl will only need to use signatures and hashes.
- Identity is defined as a set of keys managed with humans in the loop.

## Non-Goals
- Format or protocol specifications or interoperability with other implementations.
This is the only implementation and it can be embedded or called into by clients.
- Blockchain, DHT or other public global state.
Owl will only ever exchange messages directly with known peers using INET256.
- Any concept of identity without humans in the loop.
If Alice says that Bob is addresses x, y, and z, then that's who Bob is to her.
There will be no global namespace, ledger or other authority to tell her otherwise.
Messages from those addresses will appear as coming from Bob, and her Owl instance will sync channels with those addresses.

## Not-Yet Goals
- Voice/Video calling.
INET256 provides secure, unordered, and unreliable delivery, which is ideal for real time applications.
- A reasonable solution to [xkcd.com/949](https://xkcd.com/949)

## Getting Started
Owl is written in Go.  The entrypoint is in `cmd/owl`.
You can run it with `go run ./cmd/owl`, install with `go install ./cmd/owl/`.

Read the [CLI Docs](./doc/20_CLI.md) to learn more about how to use the `owl` command.

Read the [API Docs](./doc/30_API.md) to learn how to interact with a running owl instance.

## More
Support and discussion happen in the INET256 discord
[<img src="https://discord.com/assets/cb48d2a8d4991281d7a6a95d2f58195e.svg" width="80">](https://discord.gg/TWy6aVWJ7f)
