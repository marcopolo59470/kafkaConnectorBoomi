
package com.boomi.connector.kafka.operation.commit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public interface Committable {

    /**
     * @return the {@link TopicPartition} where to commit
     */
    TopicPartition getTopicPartition();

    /**
     * @return the offset position to commit
     */
    OffsetAndMetadata getNextOffset();
}
