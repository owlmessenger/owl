# Directories

A Directory is a listing of Channels.

Whenever a user creates, joins, or deletes a channel: they are modifying a Directory which tracks that information.

Each Persona in Owl has a single Directory, which is synchronized between peers.
If you join a Channel on your phone, you will also see that take effect on other devices in the same Persona.

At the time of writing the only methods which manipulate a Persona's directory are `{Create, Join, Delete}Channel`.  So the concept of directory is not directly exposed.  But it's good to have a term for "the thing that syncs the channels across devices"
