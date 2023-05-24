# Malestrom G-Set

Implements the [maelstrom g-set exercise][g-set-exercice]

[g-set-exercice]:https://github.com/jepsen-io/maelstrom/blob/main/doc/04-crdts/01-g-set.md

## Objectives

Implement a grow only replicated set, aka: [g-set](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type#G-Set_(Grow-only_Set))

## Solution

Similar to the the broadcast challenge we will replicate the items in the set
that each node keeps in memory, but instead of replicating the items 1 by 1, we
will replicate all the set each time to all the nodes in the topology.
