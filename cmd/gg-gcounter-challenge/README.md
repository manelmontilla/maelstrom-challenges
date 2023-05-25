# Challenge 4: Grow-Only Counter

[Challenge #4: Grow only counter](https://fly.io/dist-sys/4/)

## Objectives

Implement a grow-only counter using the [Maelstrom sequentially consistent service][maelstrom-seq-kv]

## Solution

Each node stores a tuple (value, version) in the same key of the store. The
value contains the sum of the deltas received so far, while the version
corresponds to the number of times the value has been increased.

A node uses the following logic to update the value avoiding data races
between writes:

1. Read the current pair (value, version) from the store
2. Update the pair to (value + delta, version++)
3. Store the pair, if and only if, the current pair (value, version) in the
   store is the same we read in the step 1
4. If the update was ok, finish the operation
5. If the update was not ok start again from step 1

The g-counter workload expects that all the nodes return the last version of the
counter after some time. In order to fulfill that requirement, when a node
receives a read operation, it reads the value from the kv store, asks the other
nodes to read the value from the store, and returns the value with the highest
version number. Technicaly we could just store the value without a version and
return the max value of all the nodes, but using a version, allows to use this
solution to things more complex than a grow only counnter.

[maelstrom-seq-kv]:https://github.com/jepsen-io/maelstrom/blob/main/doc/services.md#seq-kv
