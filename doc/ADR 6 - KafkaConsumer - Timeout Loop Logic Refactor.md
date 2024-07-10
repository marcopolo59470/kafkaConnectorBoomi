# ADR #6 - KafkaConsumer - Remove nested timeout loops

JIRA Ticket: https://boomii.atlassian.net/browse/BOOMI-39270

## Kafka Library & Consume Operation

The `KafkaConsumer#poll(long timeout)` method on Kafka Official Library orchestrates an iteration mechanism to keep requesting messages to its assigned topic until at least one message is available or the given timeout is exhausted. 

The Consume Operation in the connector also maintains its own looping mechanism to gather messages until the criteria (time & number of messages) configured by the user are met.

These nested loops were discussed during the first release of the connector and it was agreed to consider it a technical debt that should be addressed for phase 2.

## Decision

As the loop in the connector is necessary to accomplish the operation requirements, it was decided to avoid the iteration in the library by exposing a custom poll method: `KafkaConsumer#singlePoll(long timeout)`.