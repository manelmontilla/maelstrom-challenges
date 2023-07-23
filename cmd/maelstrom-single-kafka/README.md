# Malestrom Single-Node Kafka

Implements the [single node kafka exercise][single-kafka].

## Objectives

Implement a log service with an API similar to Kafka. This first iteration of
the exercise only requires to work with a single node.

## Solution

As the solution only has to work in a single node, it basically holds two maps
in the memory of the node. One map stores the logs by key and each value of the
map represents the values using a slice of ints. The other map stores the
committed logs using the key to identify the log and each value is just an int
storing the last committed offset for that key. I decided to make the solution
to use channels to syncronice the access in the structures instead of using the
more usual mutexes, because I wanted to evaluate if the code could be cleaner
of or not in that way. It looks it isn't so I probably continue using mutexes
for this kind of structures.

[single-kafka]:https://fly.io/dist-sys/5a/
