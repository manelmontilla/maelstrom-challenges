# Malestrom Multi-Node Kafka

Implements the [multi node kafka exercise][multi-kafka].

## Objectives

Implement a log service with an API similar to Kafka, but this time
allowing more than 1 node running at the same time.

## Solution

The solution uses the key value stores provided by Maelstrom.

The offsets of the topics use distributed sequences backed by the sequential
consistent key value store. As we use a CAS operation loop for getting the next
offset of a topic a sequential consistency is kv store is enough.

The committed offsets are also using the sequential consistent kv store.
Theoretically, they should also use a linearizable storage to avoid possible
stale reads, but running the Maelstrom tests specified in the challenge don't
detect any anomaly using the sequential one.

The values of a topic are stored using a linearizable kv store, as we need the
write and read operations to be ordered according to a wall-clock time.

[multi-kafka]:https://fly.io/dist-sys/5b/
