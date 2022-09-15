# Personas

Personas in Owl are like accounts in a centralized system.

Personas allow users to compartmentalize different parts of their lives to avoid unwanted leaking of information between social contexts.
For example: someone who uses Owl personally and professionally might have different Persona's to encapsulate each.
Their work laptop could have the work Persona, and their personal laptop: the personal Persona.
Their phone might have both Personas.

Some definitions for the upcoming section:
- *Instance* - A running owl application and its state. Instances of owl have their own private state that is not shared with other instances.
- *Peer* - an entity available over the network with a cryptographically stable identity.

Personas have no global identifier; they are identified by locally unique names within an Owl instance.
Even though there is no global identifier, a Persona has a set of peers which are also considered part of that Persona.
That set of peers could be different on different devices.
Instance A could think that peers B and C are part of the same Persona, but the device with peer B, might only acknowledge peer A and not C.

So to recap:
- Instances have multiple Personas
- Personas have multiple Peers
- Some of the peers (usually just 1) are active on the local instance.  The rest of the peers are on other instances.

## Creating a Persona
Creating a persona is something that a user does the first time they use Owl.

Except in the case of technical bugs or user mistakes, this will only be done a few times.

## Adding a new Device
Joining refers to creating a persona locally with remote peers that are already representing an existing persona.

Expanding refers to adding another peer to an existing persona.
