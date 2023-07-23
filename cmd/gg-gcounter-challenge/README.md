# Challenge 4: Grow-Only Counter

[Challenge #4: Grow only counter](https://fly.io/dist-sys/4/)

## Objectives

Implement a grow-only counter using the [Maelstrom sequentially consistent service][maelstrom-seq-kv]

## Solution

The solution uses stores the value of the counter together with the node id that wrote that value
in the key-value store. For updating the node it uses the following logic to avoid the data race
between writes from different nodes:

1. Read the current pair (value, node-id that set this last value) from the store
2. Update the pair to (value+delta, node-id of the current node performing the operation)
3. Try to update the pair of the store to the new one, if and only if, the pair
(value, node-id)
is the same we read in step 1
4. If the update was ok, finnish the operation
5. If the update was not ok start again from step 1

[maelstrom-seq-kv]:https://github.com/jepsen-io/maelstrom/blob/main/doc/services.md#seq-kv
