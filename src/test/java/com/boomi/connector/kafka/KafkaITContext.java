
package com.boomi.connector.kafka;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.kafka.configuration.SASLMechanism;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.testutil.ConnectorTestContext;
import com.boomi.connector.testutil.SimpleAtomConfig;

import org.apache.kafka.common.security.auth.SecurityProtocol;

public class KafkaITContext extends ConnectorTestContext {

    public static final String VM_HOST = "34.238.232.20";

    public static final String VM_WITH_SHA256_PLAINTEXT = VM_HOST + ":9292";
    public static final String VM_WITH_SSL = VM_HOST + ":9192";

    private static final String VM_SERVER = VM_HOST + ":9092";
    private static final String VM_BLANK_USERNAME = null;
    private static final String VM_BLANK_PASSWORD = null;
    private static final String VM_MECHANISM = SASLMechanism.NONE.getMechanism();
    private static final String VM_PROTOCOL = SecurityProtocol.PLAINTEXT.name;
    private static final String CONSUMER_GROUP = "consume integration tests";

    public static KafkaITContext produceVM() {
        KafkaITContext context = new KafkaITContext();
        addVMConnectionProperties(context);

        context.addOperationProperty(Constants.KEY_CLIENT_ID, "produce integration tests");
        setProduceDefaults(context);

        context.setOperationType(OperationType.EXECUTE);
        context.setOperationCustomType(CustomOperationType.PRODUCE.name());

        return context;
    }

    private static void setProduceDefaults(KafkaITContext context) {
        context.addOperationProperty(Constants.KEY_ACKS, "0");
        context.addOperationProperty(Constants.KEY_COMPRESSION_TYPE, "none");
        context.addOperationProperty(Constants.KEY_MAXIMUM_TIME_TO_WAIT, 30000L);
    }

    public static KafkaITContext consumeVM() {
        KafkaITContext context = new KafkaITContext();
        addVMConnectionProperties(context);

        context.addOperationProperty(Constants.KEY_CLIENT_ID, CONSUMER_GROUP);
        context.addOperationProperty(Constants.KEY_RECEIVE_MESSAGE_TIMEOUT, 30000L);
        context.addOperationProperty(Constants.KEY_MIN_MESSAGES, 1L);
        context.addOperationProperty(Constants.KEY_AUTOCOMMIT, false);
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, CONSUMER_GROUP);
        context.addOperationProperty(Constants.KEY_AUTO_OFFSET_RESET, "earliest");

        context.setOperationType(OperationType.EXECUTE);
        context.setOperationCustomType(CustomOperationType.CONSUME.name());

        return context;
    }

    public static KafkaITContext commitOffsetVM() {
        KafkaITContext context = new KafkaITContext();
        addVMConnectionProperties(context);

        context.addOperationProperty(Constants.KEY_CLIENT_ID, CONSUMER_GROUP);
        context.addOperationProperty(Constants.KEY_CONSUMER_GROUP, CONSUMER_GROUP);

        context.setOperationType(OperationType.EXECUTE);
        context.setOperationCustomType(CustomOperationType.COMMIT_OFFSET.name());

        return context;
    }

    public static KafkaITContext adminVMClient() {
        KafkaITContext context = new KafkaITContext();
        context.addOperationProperty(Constants.KEY_CLIENT_ID, "adminClientIT");
        addVMConnectionProperties(context);

        return context;
    }

    public static KafkaITContext addVMConnectionProperties(KafkaITContext context) {
        context.addConnectionProperty(Constants.KEY_USERNAME, VM_BLANK_USERNAME);
        context.addConnectionProperty(Constants.KEY_PASSWORD, VM_BLANK_PASSWORD);
        context.addConnectionProperty(Constants.KEY_SERVERS, VM_SERVER);
        context.addConnectionProperty(Constants.KEY_SECURITY_PROTOCOL, VM_PROTOCOL);
        context.addConnectionProperty(Constants.KEY_SASL_MECHANISM, VM_MECHANISM);

        return context;
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return KafkaConnector.class;
    }

    public KafkaITContext setProduce() {
        setOperationCustomType(CustomOperationType.PRODUCE.name());
        setOperationType(OperationType.EXECUTE);
        setProduceDefaults(this);
        return this;
    }

    public KafkaITContext setConsume() {
        setOperationCustomType(CustomOperationType.CONSUME.name());
        setOperationType(OperationType.EXECUTE);
        return this;
    }

    public KafkaITContext setCommit() {
        setOperationCustomType(CustomOperationType.COMMIT_OFFSET.name());
        setOperationType(OperationType.EXECUTE);
        return this;
    }

    public KafkaITContext setTestConnection() {
        setOperationCustomType(CustomOperationType.TEST_CONNECTION.name());
        return this;
    }

    public KafkaITContext withConnectionProperty(String key, Object value) {
        addConnectionProperty(key, value);
        return this;
    }

    public KafkaITContext withContainerProperty(String key, String value) {
        SimpleAtomConfig config = (SimpleAtomConfig) getConfig();
        config.withContainerProperty(key, value);
        return this;
    }
}
