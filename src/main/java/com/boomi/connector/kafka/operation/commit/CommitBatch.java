
package com.boomi.connector.kafka.operation.commit;

import com.boomi.connector.api.ObjectData;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

class CommitBatch {

    private final Set<ObjectData> _documents = new HashSet<>();
    private CommitMessage _messageToCommit;

    void update(CommitMessage message, ObjectData document) {
        _documents.add(document);

        if (_messageToCommit == null || _messageToCommit.isBefore(message)) {
            _messageToCommit = message;
        }
    }

    CommitMessage getMessageToCommit() {
        return _messageToCommit;
    }

    Collection<ObjectData> getDocuments() {
        return _documents;
    }
}
