# Owl

Owl is packaged as a command line tool which can run a server process exposing an API, or directly view and manipulate an instance's state.

This section describes the commands provided by Owl.

### `owl chat <persona> <channel-name>`

## Server

### `owl serve <laddr>`

## Personas

### `owl persona ls`

### `owl persona create <name>`

### `owl persona drop <name>`

### `owl persona join <name> <peers...>`

### `owl persona add-peer <name> <peers...>`

### `owl persona rm-peer <name> <peers...>`

## Contacts

### `owl contact ls`

### `owl contact create <name> <peers...>`

### `owl contact delete <name>`

### `owl contact add-peer <name> <peers...>`

### `owl contact rm-peer <name> <peers...>`

## Channels

### `owl channel ls <persona>`

### `owl channel create <persona> <name>`

### `owl channel delete <persona> <name>`

### `owl channel join <persona> <uid>`

### `owl channel invite <persona> <channel-name> <contact-name>`

### `owl channel kick <persona> <channel-name> <contact-name>`

### `owl channel read <persona> <name>`
