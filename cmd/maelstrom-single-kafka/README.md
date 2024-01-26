# Malestrom Single-Node Kafka

Implements the [single node kafka exercise][single-kafka].

## Objectives

Implement a log service with an API similar to Kafka. This first iteration of
the exercise only requires to work with a single node.

## Solution

As the solution only needs to work in a single node, holding two maps in the
memory of the node suffices. One map stores the logs by key. Each value of
the map represents the values of the log using a slice of int's. The other map
stores the committed logs. It uses the name of the log as a the key of the map,
and each value is just an int storing the last committed offset for that key. I
decided to make the solution to use channels to synchronize the access in the
structures instead of using the more usual mutexes, just to have some fun.

[single-kafka]:https://fly.io/dist-sys/5a/
