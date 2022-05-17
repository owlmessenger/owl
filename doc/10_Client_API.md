# Owl Client API

Owl's client API includes 4 main concepts: *Personas, Contacts, Channels, and Events*.

## Persona
A persona is like an account or email address. 
A user might only have one of these which would be shared across devices.
A persona is a set of PeerID's spread across multiple devices.

## Contact
Each persona has a set of contacts uniquely identified by a name.
A contact can be thought of as a reference to someone else's persona.

## Channels
Each persona has a set of channels which it participates in.
Channels contain events, some events are messages.
And messages are the whole point of all this.
Events are accessed with an EventPath.

### EventPath
An EventPath is a list of 64 bit integers with `length >= 1`.
Channels are organized into threads of arbitrary depth.
Events in the root of the channel are part of the main flow of the conversation.
They have a EventPath with `length = 1`.

Messages in a thread would have a EventPath of `length > 1`.
Threads can have arbitrarily nested sub threads, a thread within a thread would have EventPaths with `length = 2`.
