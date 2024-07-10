
package com.boomi.connector.kafka.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.kafka.KafkaITContext;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class CustomOperationTypeTest {

    @DataPoints
    public static String[] VALID_TYPES = new String[] { "PRODUCE", "TEST_CONNECTION", "CONSUME", "COMMIT_OFFSET" };

    @Theory
    public void valueOfTest(String type) {
        KafkaITContext context = new KafkaITContext();
        context.setOperationCustomType(type);

        CustomOperationType operationType = CustomOperationType.fromContext(context);
        Assert.assertEquals(type, operationType.name());
    }

    @Test(expected = ConnectorException.class)
    public void invalidValueOfShouldThrowTest() {
        KafkaITContext context = new KafkaITContext();
        context.setOperationCustomType("invalid");

        CustomOperationType.fromContext(context);
    }
}