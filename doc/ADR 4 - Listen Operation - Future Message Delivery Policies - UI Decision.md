# ADR #4 - Listen Operation - Future Message Delivery Policies: UI Decision

JIRA Ticket: https://boomii.atlassian.net/browse/BOOMI-42594

## Current Implementation

On ADR #3, it was decided to implement only an **At least once** delivery for the MVP release. However, it is not discarded to implement other delivery semantics in the future.

## Architecture Decision

A dropdown labeled **Message Delivery Policy** will be added to the operation properties configuration. 
For the MVP, it will display only one option labeled **At least once**. 
This will allow the inclusion of new delivery semantics, without changing the UI of the connector.

