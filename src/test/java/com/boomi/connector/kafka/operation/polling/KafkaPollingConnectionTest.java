//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation.polling;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.util.Constants;

import org.junit.Assert;
import org.junit.Test;

public class KafkaPollingConnectionTest {

    /**
     * Validates the operation properties for {@link KafkaPollingOperation}
     */
    @Test
    public void operationPropertiesTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        final long maxPollRecords = 57L;

        // set operation properties
        context.addOperationProperty(Constants.KEY_MAX_MESSAGES, maxPollRecords);

        // instantiate new connection
        KafkaPollingConnection connection = new KafkaPollingConnection(context);

        // assert values
        Assert.assertEquals(maxPollRecords, connection.getMaxPollRecords());
    }

    /**
     * Validates operation property {@code max_messages_poll} cannot be less than 1.
     */
    @Test(expected = ConnectorException.class)
    public void invalidOperationPropertiesTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        final long maxPollRecords = 0L;

        // set operation properties
        context.addOperationProperty(Constants.KEY_MAX_MESSAGES, maxPollRecords);

        // instantiate new connection
        KafkaPollingConnection connection = new KafkaPollingConnection(context);

        // get invalid property
        connection.getMaxPollRecords();
    }

    @Test
    public void getDelayPropertyTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        final long delay = 42000L;

        // set delay property
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);

        // instantiate new connection
        KafkaPollingConnection connection = new KafkaPollingConnection(context);

        // assert values
        Assert.assertEquals(delay, connection.getPollingDelay());
    }

    @Test
    public void getIntervalPropertyTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        final long interval = 45000L;

        // set delay property
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        // instantiate new connection
        KafkaPollingConnection connection = new KafkaPollingConnection(context);

        // assert values
        Assert.assertEquals(interval, connection.getPollingInterval());
    }

    @Test
    public void getDelayPropertyDefaultValueTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        // instantiate new connection
        KafkaPollingConnection connection = new KafkaPollingConnection(context);

        // assert values
        Assert.assertEquals(Constants.DEFAULT_POLLING_DELAY, connection.getPollingDelay());
    }

    @Test
    public void getIntervalPropertyDefaultValueTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        // instantiate new connection
        KafkaPollingConnection connection = new KafkaPollingConnection(context);

        // assert values
        Assert.assertEquals(Constants.DEFAULT_POLLING_INTERVAL, connection.getPollingInterval());
    }

}
