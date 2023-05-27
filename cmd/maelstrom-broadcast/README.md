# Challenge 3: Efficient Broadcast

[Challenge #3e: Efficient Broadcast, Part II](https://fly.io/dist-sys/3e/)

## Objectives

With the same node count of 25 and a message delay of 100ms, your challenge is
to achieve the following performance metrics:

Messages-per-operation is below 20
Median latency is below 1 second
Maximum latency is below 2 seconds

## Solution

1. Accumulate messages for the same destination during a defined period of time
and send them in batches

2. Store together with each message (messages are identified using integers) a
list indicating the nodes that we know already have the messages. Send the
bit field in each broadcast message and update it with the one received

## Solution stats

Using windows of 150 ms for aggregating messages.

- Messages-per-operation: 11.656922
- Latencies:
Median: 751 ms
Maximum: 1208 ms
