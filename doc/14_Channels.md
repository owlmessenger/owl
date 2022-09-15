# Channels

Channels are an API only concept in Owl.
A Channel is a place to send and receieve messages, but it does not necessarily map to any single implementation within Owl.

The implementation for a direct message vs a public chat room are different, but they both appear as channels through the API, and you could manipulate them using the same APIs.

Channels can be thought of as a map from EntryPaths to Entries.

## EntryPaths
An EntryPath is a list of unsigned integers, which is capable of modeling nested threads.
Channels are organized into threads of arbitrary depth.

Entrys in the root of the channel are part of the main flow of the conversation.
They have a EntryPath with `length = 1`.

e.g.
```
PATH    USER    MESSAGE

[0]     Alice   "hello"
[1]     Bob     "hi"
[1, 0]  Alice   "created thread from hi"
```

Messages in a thread would have a EntryPath of `length > 1`.
Threads can have arbitrarily nested sub threads, a thread within a thread would have EntryPaths with `length > 2`.
