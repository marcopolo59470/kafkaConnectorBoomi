# ADR #5 - Listen Operation - Not using a ConsumerRebalanceListener

JIRA Ticket: https://boomii.atlassian.net/browse/BOOMI-42594

## Partition Rebalance

The official Kafka Client offers the possibility of implementing a ConsumerRebalanceListener that will pass as an argument when the Consumer is subscribed to a Topic. This mechanism is useful to avoid triggering errors on committing or rewinding message offsets when a partition rebalance is in progress.

## Implementation Decision

It was decided to not implement a ConsumerRebalanceListener on the final version of the operation MVP. 
The main reason for this was the decision of only supporting **At least once** delivery (ADR #3), avoiding the need of an exhaustive control over Partition Rebalances.
Because of the long-lived nature of the listen operation and the semantics of **At least once** delivery, an error triggered by a Partition Rebalance when committing or rewinding messages does not imply the loss of messages.

### Commit Offset while a Partition Rebalance is in progress

Any attempt to commit while a Partition Rebalance is in progress or to a Partition that is no longer assigned to that particular consumer will fail. 
This scenario is not a problem for the operation given that the next commit for a message batch will effectively commit the previous offsets from the same partition too.
In the same way, it will not be an issue for those partitions that were revoked from the consumer, as the messages will be processed again by the next client that is assigned with those partitions.

### Rewind Offset while a Partition Rebalance is in progress

Similar to commit, invoking a rewind offset when a Partition Rebalance is in progress or after an assignment was revoked, will end up triggering an error for the revoked partitions.
This scenario is not a problem because the messages from the revoked partition will be retrieved again by the next client that is assigned with those partitions. 
On the other hand, as the rewind is done on a per partition basis, the partitions that are still assigned to the consumer will be rewound and the messages will be retrieved again in the next poll.
