
package com.boomi.connector.kafka;

import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.commit.CommitOffsetOperation;
import com.boomi.connector.kafka.operation.consume.ConsumeOperation;
import com.boomi.connector.kafka.operation.produce.ProduceOperation;
import com.boomi.connector.kafka.util.Constants;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

@RunWith(Theories.class)
public class KafkaConnectorTest {

    @DataPoints(value = "ctxs and operations")
    public static Collection<ContextAndOperationVO> contextsAndOperations() {
        Collection<ContextAndOperationVO> params = new ArrayList<>(3);

        // Consume
        KafkaITContext consumeContext = KafkaITContext.consumeVM();
        consumeContext.setOperationCustomType(CustomOperationType.CONSUME.name());
        consumeContext.setOperationType(OperationType.EXECUTE);
        consumeContext.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);

        params.add(new ContextAndOperationVO(consumeContext, ConsumeOperation.class));

        // Produce
        KafkaITContext produceContext = KafkaITContext.produceVM();
        produceContext.setOperationCustomType(CustomOperationType.PRODUCE.name());
        produceContext.setOperationType(OperationType.EXECUTE);
        produceContext.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);

        params.add(new ContextAndOperationVO(produceContext, ProduceOperation.class));

        // Commit Offset
        KafkaITContext commitOffsetContext = KafkaITContext.commitOffsetVM();
        commitOffsetContext.setOperationCustomType(CustomOperationType.COMMIT_OFFSET.name());
        commitOffsetContext.setOperationType(OperationType.EXECUTE);
        commitOffsetContext.setObjectTypeId(Constants.DYNAMIC_TOPIC_ID);

        params.add(new ContextAndOperationVO(commitOffsetContext, CommitOffsetOperation.class));

        return params;
    }

    @Test
    public void createBrowserTest() {
        KafkaConnector kafkaConnector = new KafkaConnector();
        Browser browser = kafkaConnector.createBrowser(KafkaITContext.produceVM());

        Assert.assertTrue(browser instanceof KafkaBrowser);
    }

    @Theory
    public void createOperation(@FromDataPoints("ctxs and operations") ContextAndOperationVO contextAndOperationVO) {
        KafkaConnector connector = new KafkaConnector();
        Operation operation = connector.createExecuteOperation(contextAndOperationVO._context);

        Assert.assertNotNull(operation);
        Assert.assertTrue(contextAndOperationVO._operation.isInstance(operation));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void createInvalidOperation() {
        KafkaITContext context = KafkaITContext.consumeVM();
        context.setOperationCustomType(CustomOperationType.TEST_CONNECTION.name());
        context.setOperationType(OperationType.EXECUTE);

        KafkaConnector connector = new KafkaConnector();

        connector.createExecuteOperation(context);
    }

    private static class ContextAndOperationVO {

        private final KafkaITContext _context;
        private final Class<? extends Operation> _operation;

        ContextAndOperationVO(KafkaITContext context, Class<? extends Operation> operation) {
            _context = context;
            _operation = operation;
        }
    }
}
