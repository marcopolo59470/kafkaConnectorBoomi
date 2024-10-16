package com.boomi.connector.kafka;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.Browser;
import com.boomi.connector.api.Operation;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.KafkaOperationConnection;
import com.boomi.connector.kafka.operation.commit.CommitOffsetOperation;
import com.boomi.connector.kafka.operation.consume.ConsumeOperation;
import com.boomi.connector.kafka.operation.polling.KafkaPollingConnection;
import com.boomi.connector.kafka.operation.polling.KafkaPollingOperation;
import com.boomi.connector.kafka.operation.produce.ProduceOperation;
import com.boomi.connector.util.listen.UnmanagedListenConnector;
import com.boomi.connector.util.listen.UnmanagedListenOperation;

public class KafkaConnector extends UnmanagedListenConnector {

    /**
     * Creates a {@link KafkaBrowser} using the given context
     *
     * @param context
     *         the context
     * @return a {@link KafkaBrowser}
     */
    @Override
    public Browser createBrowser(BrowseContext context) {
        return new KafkaBrowser(new KafkaConnection<>(context));
    }

    /**
     * Creates the appropriate {@link Operation} depending on the Custom Operation Type defined on the context
     *
     * @param context
     *         to get the Custom Operation Type
     * @return the {@link Operation}
     */
    @Override
    public Operation createExecuteOperation(OperationContext context) {
        CustomOperationType operationType = CustomOperationType.fromContext(context);
        KafkaOperationConnection connection = new KafkaOperationConnection(context);

        switch (operationType) {
            case PRODUCE:
                return new ProduceOperation(connection);
            case CONSUME:
                return new ConsumeOperation(connection);
            case COMMIT_OFFSET:
                return new CommitOffsetOperation(connection);
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public UnmanagedListenOperation createListenOperation(OperationContext context) {
        return new KafkaPollingOperation(new KafkaPollingConnection(context));
    }
}
