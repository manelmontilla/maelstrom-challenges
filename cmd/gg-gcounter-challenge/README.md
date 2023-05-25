# Challenge 4: Grow-Only Counter

[Challenge #4: Grow only counter](https://fly.io/dist-sys/4/)

## Objectives

Implement a grow-only counter using the [Maelstrom sequentially consistent service][maelstrom-seq-kv]

## Solution

Each node stores the value of the counter together with a version field that is increased
by 1 each time value is update in a single key shared by all the nodes. 

For updating the node it uses the following logic to avoid the data race
between writes from different nodes:

1. Read the current pair (value, version) from the store
2. Update the pair to (value+delta, version++)
3. Store the pair, if and only if, the current pair (value, version) in the store is the same we read in step 1
4. If the update was ok, finnish the operation
5. If the update was not ok start again from step 1

The g-counter workload expects that all the nodes return the last version of the
counter after some time. In order to fulfill that requirement, when a node
receives a read operation it reads the value from the kv store, asks the other
nodes to read the value from the store, and returns the value with the highest
version number. Technicaly we could just store the value without a version and
return the max value of all the nodes, but using a version, allows to use this
solution to things more complex than a grow only counnter.

[maelstrom-seq-kv]:https://github.com/jepsen-io/maelstrom/blob/main/doc/services.md#seq-kv
