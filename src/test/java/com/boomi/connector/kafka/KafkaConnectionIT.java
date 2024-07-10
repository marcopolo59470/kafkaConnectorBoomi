
package com.boomi.connector.kafka;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.kafka.operation.CustomOperationType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class KafkaConnectionIT {

    @Test
    public void getTopicsTest() {
        KafkaITContext context = new KafkaITContext();
        context.setOperationCustomType(CustomOperationType.TEST_CONNECTION.name());
        KafkaITContext.addVMConnectionProperties(context);

        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);

        Set<String> topics = connection.getTopics();

        Assert.assertNotNull(topics);
        Assert.assertFalse(topics.isEmpty());
    }

}
