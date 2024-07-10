# ADR #3 - Listen Operation - Message Delivery Policy: At Least Once

JIRA Ticket: https://boomii.atlassian.net/browse/BOOMI-42594

## General Concepts

### Offset Management
A distinctive characteristic of Apache Kafka with respect to other Pub/Sub services is that, instead of relying on the acknowledgment of individual messages, a Kafka consumer keeps track of the progress for each partition with an Offset Management that allows Consumers to advance or retrocede several positions at a time.
Although this mechanism provides a high level of flexibility, it also requires a carefully planned commit strategy to reduce the possibility of either losing or duplicating messages by committing the offsets in an incorrect order.

### Delivery Semantics
There are different delivery semantics that can be achieved by a correct configuration of the different components of the messaging system, with a special focus on the adopted offset management strategy:

- **At most once**: A message should be delivered only once. It's acceptable to lose a message rather than delivering it twice. A few use cases of at most once includes metrics collection, log collection, and so on. Applications adopting at most once semantics can easily achieve higher throughput and low latency.

- **At least once**: It is acceptable to deliver a message more than once but no messages should be lost. The Consumer ensures that messages have been processed for sure, even though it may result in message duplication. This is the most preferred semantics system out of them all. Applications adopting at least once semantics may have moderate throughput and moderate latency.

- **Exactly once**: A message must be delivered only once and no message should be lost. This semantic is the hardest to achieve as it requires a correct end-to-end configuration and may have lower throughput and higher latency than the other two semantic systems we've looked at.

## Implementation Considerations

### SDK Listen Operation
The behavior of  Listen Operation implemented with the Connector SDK differs from a normal Boomi process as follows:

1. Even though the Listen Operation is configured as the Starting shape of a process, it’s actually a long-lived component that will submit new process executions whenever an event is received.

2. It is possible to submit multiple concurrent process executions for individual events or to collect them into a PayloadBatch and submit up to 5 concurrent executions.

3. Process executions are Asynchronous and even though they cannot be controlled by the ListenOperation once they are submitted, it’s possible to configure them in such a way that it’s possible to ask for its final status (success/failure).


### Kafka Listen Operation
The aforementioned Offsets Management of Kafka in conjunction with the asynchronous nature of a Listen Operation creates a scenario where achieving a specific Delivery Semantic can be challenging because the order in which the process executions will be completed cannot be assured.

In order to circumvent this complexity and considering that the requirements recollected from potential customers was to provide an “at-least-once” delivery, it was decided to implement the Operation in the following way:

- Disable the autocommit functionality of the official Kafka client in favor of a Connector managed commission of the offsets.

- Limit the number of concurrent process executions to 1.

- Submit batches in such a way that it’s possible to track their Offsets and get its final status after the execution is completed.

- Mitigate the performance constraint of limiting the number of concurrent executions by allowing the deployment of multiple parallel consumers in different containers.

## Message Delivery Policy: At Least Once

This delivery policy is intended to provide an **At least once** processing by verifying the final state of every process execution and acting accordingly by:

- **Successful execution**: Commit Offsets belonging to partitions that are still assigned to the consumer.

- **Failed executions**: Rewinding the offsets to the last message that was processed successfully on each partition.

### Expected Implications

This mechanism will prevent the loss of messages and guarantee that all messages retrieved by the operation are processed successfully before committing them. 
As a consequence, It’s possible to have duplicate messages if a rebalance takes place after the PayloadBatch is built but prior to its completion.

The operation invokes commit or rewind of message offsets depending on the result of the process execution. 
if a process execution returns a failure state, the connector will rewind the offsets to re-submit them into a new execution, effectively acting as a retry mechanism whose exit condition depends on the result of the process.
This behavior is mandatory to provide **At least once** guarantees and it should be included in the user documentation so customers can build their processes accordingly, generating an exit condition and making sure the listener does not end up looping over the same messages.
