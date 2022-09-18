# API

Owl serves a JSON-RPC 2.0 API as a means of communicating with a running instance.
This is the preferred way of using Owl since applications can be notified of incoming messages as they happen.

There is a [Go client](../pkg/owljsonrpc/client.go) available for this API.

If you are using another lanaguage you can get an idea for what the API produces by looking at the interfaces [here](../pkg/owl/api.go).
The names of the JSON fields are annotated on the `structs` in Go.
