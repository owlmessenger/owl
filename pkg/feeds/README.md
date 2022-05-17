# Feed
A feed is a directed graph.
A minimal set of source nodes needed to reach the rest of the graph are called the *Heads*

The graph can be mutated by actors called *Peers*.
The feed is created with an initial set of peers.

Peers are constantly sharing their view of the graph with one another.

Peers must only increase the value of N.
Peers must keep track of one another last used N and reject messages which do not increase it by 1.
When a peer appends to the feed, they reduce the number of Heads to 1.

## Validation
Nodes and the CA store that holds them must adhear to several invariants.

A node is considered valid if it:
- Only references valid nodes
- References nodes which exist
- Has a correct N, relative to it's ancestors
- If N is 1, then there must be 1 previous node and it's ID must match the feed ID.
- Does not contain invalid operations, such as adding or removing incorrect peers, or writing data without permission.

The store backing the feed only contains valid nodes, and nodes must always be checked for validity before being added.

## Verification/Authentication
Nodes are considered authentic if one of two criteria are met:
- It is reachable from any of the heads advertised by its Author.
- It is reachable from any other authentic message by the same Author.

Legitamate Peers only append nodes to the authentic portion of a feed.
Inauthentic subgraphs will never be referenced.
