//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation.polling;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.listen.ListenManager;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.api.listen.PayloadBatch;
import com.boomi.connector.api.listen.SingletonListenOperation;
import com.boomi.connector.api.listen.SubmitOptions;
import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.operation.consume.BoomiConsumer;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.io.FastByteArrayInputStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class KafkaPollingOperationTest {

    private static final boolean FINISH_PROCESS_EXECUTION = true;
    private static final boolean DO_NOT_FINISH_EXECUTION = false;
    private static final boolean FAILED = false;

    private static final int NEVER = 0;
    private static final int ONCE = 1;
    private static final int TWICE = 2;

    @Test
    public void startShouldSubscribeConsumerAndScheduleExecutorTest() {
        final long delay = 2700L;
        final long interval = 6200L;

        BoomiListenerConsumer consumer = mockConsumer();
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        operation.start(createListenerMock());

        verify(executor, times(ONCE)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(ONCE)).subscribe();
        verify(consumer, times(NEVER)).close();
    }

    @Test
    public void failedOnStartShouldCloseConsumerTest() {
        final long delay = 2500L;
        final long interval = 7800L;

        // consumer mock throws exception on subscribe
        BoomiListenerConsumer consumer = mockConsumer();
        Mockito.doThrow(new RuntimeException()).when(consumer).subscribe();

        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        try {
            operation.start(createListenerMock());
        } catch (RuntimeException e) {
            // do nothing
        }

        verify(executor, times(NEVER)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(ONCE)).subscribe();
        verify(consumer, times(ONCE)).close();
    }

    @Test
    public void stopShouldCloseResourcesTest() {
        final long delay = 6600L;
        final long interval = 6800L;

        BoomiListenerConsumer consumer = mockConsumer();

        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        operation.stop();

        verify(executor, times(ONCE)).shutdown();
        verify(consumer, times(ONCE)).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void runTest() throws ExecutionException, InterruptedException {
        // prepare operation

        final long delay = 2700L;
        final long interval = 6200L;

        BoomiListenerConsumer consumer = mockConsumer();
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        PayloadBatchDouble payloadBatch = new PayloadBatchDouble();
        Listener listenerMock = createListenerMock(payloadBatch);
        operation.start(listenerMock);

        verify(executor, times(ONCE)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(ONCE)).subscribe();
        verify(consumer, times(NEVER)).close();

        // run operation

        when(consumer.isSubscribed()).thenReturn(true);

        operation.run();

        verify(listenerMock, times(ONCE)).getBatch();
        assertTrue("Payload should had been submitted", payloadBatch._isSubmitted);
        assertEquals(10, payloadBatch.getCount());

        verify(consumer, times(NEVER)).commit(Mockito.anyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void runShouldNotSubmitASecondPayloadIfThereIsOnePendingTest()
            throws ExecutionException, InterruptedException {
        // prepare operation

        final long delay = 2700L;
        final long interval = 6200L;

        BoomiListenerConsumer consumer = mockConsumer();
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        PayloadBatchDouble firstPayloadBatch = new PayloadBatchDouble(DO_NOT_FINISH_EXECUTION);
        PayloadBatchDouble secondPayloadBatch = new PayloadBatchDouble();
        Listener listenerMock = createListenerMock(firstPayloadBatch, secondPayloadBatch);

        operation.start(listenerMock);

        verify(executor, times(ONCE)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(ONCE)).subscribe();
        verify(consumer, times(NEVER)).close();

        // run operation

        when(consumer.isSubscribed()).thenReturn(true);

        operation.run();

        assertTrue("Payload should had been submitted", firstPayloadBatch._isSubmitted);
        assertEquals(10, firstPayloadBatch.getCount());

        // second run
        operation.run();

        // this should be called only once as the first payload did not finished processing
        verify(listenerMock, times(ONCE)).getBatch();
        Assert.assertFalse("Payload should not had been submitted", secondPayloadBatch._isSubmitted);

        verify(consumer, times(NEVER)).commit(Mockito.anyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void runShouldSubmitNextPayloadIfPreviousOneIsFinishedTest()
            throws ExecutionException, InterruptedException {
        // prepare operation

        final long delay = 2700L;
        final long interval = 6200L;

        BoomiListenerConsumer consumer = mockConsumer();
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        PayloadBatchDouble firstPayloadBatch = new PayloadBatchDouble(FINISH_PROCESS_EXECUTION);
        PayloadBatchDouble secondPayloadBatch = new PayloadBatchDouble();
        Listener listenerMock = createListenerMock(firstPayloadBatch, secondPayloadBatch);

        operation.start(listenerMock);

        verify(executor, times(ONCE)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(ONCE)).subscribe();
        verify(consumer, times(NEVER)).close();

        // run operation

        when(consumer.isSubscribed()).thenReturn(true);

        operation.run();

        assertTrue("First Payload should had been submitted", firstPayloadBatch._isSubmitted);
        assertEquals(10, firstPayloadBatch.getCount());

        // second run
        operation.run();

        verify(listenerMock, times(TWICE)).getBatch();
        assertTrue("Second Payload should had been submitted", secondPayloadBatch._isSubmitted);
        assertEquals(10, secondPayloadBatch.getCount());

        verify(consumer, times(ONCE)).commit(Mockito.anyMap());
    }

    @Test
    public void runShouldAttemptToReconnectIfSubscriptionIsLostTest() {
        // prepare operation

        final long delay = 2700L;
        final long interval = 6200L;

        BoomiListenerConsumer consumer = mockConsumer();
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        Listener listenerMock = createListenerMock();

        operation.start(listenerMock);

        verify(executor, times(ONCE)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(NEVER)).close();

        // run operation

        when(consumer.isSubscribed()).thenReturn(false);

        operation.run();

        verify(listenerMock, times(NEVER)).getBatch();
        // the first one at start(), second one when re-subscribing
        verify(consumer, times(TWICE)).subscribe();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void runShouldRewindConsumerOnFailedBatchTest()
            throws ExecutionException, InterruptedException, IOException {
        // prepare operation

        final long delay = 2700L;
        final long interval = 6200L;

        BoomiListenerConsumer consumer = mockConsumer();
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addConnectionProperty(Constants.KEY_POLLING_DELAY, delay);
        context.addConnectionProperty(Constants.KEY_POLLING_INTERVAL, interval);

        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);

        ConnectionDouble connection = new ConnectionDouble(context, consumer, "topic");
        KafkaPollingOperation operation = new KafkaPollingOperation(connection, consumer, executor);

        PayloadBatchDouble firstBatch = new PayloadBatchDouble(FINISH_PROCESS_EXECUTION, FAILED);
        PayloadBatchDouble secondBatch = new PayloadBatchDouble();
        Listener listenerMock = createListenerMock(firstBatch, secondBatch);

        operation.start(listenerMock);

        verify(executor, times(ONCE)).scheduleAtFixedRate(operation, delay, interval,
                TimeUnit.MILLISECONDS);
        verify(consumer, times(ONCE)).subscribe();
        verify(consumer, times(NEVER)).close();

        // run operation

        when(consumer.isSubscribed()).thenReturn(true);

        operation.run();

        assertTrue("First Payload should had been submitted", firstBatch._isSubmitted);
        assertEquals(10, firstBatch.getCount());

        // second run
        operation.run();

        verify(consumer, times(ONCE)).rewind(Mockito.anyMap());
        verify(listenerMock, times(TWICE)).getBatch();
        verify(consumer, times(NEVER)).commit(Mockito.anyMap());

        assertTrue("Second Payload should had been submitted", secondBatch._isSubmitted);
        assertEquals(10, secondBatch.getCount());

        // validate the failed batch and the new one have the same payloads
        int index = 0;
        for (Payload payload : firstBatch._payloads) {
            String first = StreamUtil.toString(payload.readFrom(), StringUtil.UTF8_CHARSET);
            String second = StreamUtil.toString(secondBatch._payloads.get(index).readFrom(), StringUtil.UTF8_CHARSET);

            assertEquals(first, second);
            index++;
        }
    }

    @Test
    public void testIsSingletonProperty() {
        BoomiListenerConsumer consumer = mock(BoomiListenerConsumer.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        KafkaPollingConnection connection = mock(KafkaPollingConnection.class);
        KafkaITContext context = mock(KafkaITContext.class);

        when(context.getCustomOperationType()).thenReturn(CustomOperationType.LISTEN.name());
        when(connection.isSingletonListener()).thenReturn(true);
        when(connection.getContext()).thenReturn(context);

        SingletonListenOperation<ListenManager> pollingOperation = new KafkaPollingOperation(connection, consumer,
                executor);

        assertTrue(pollingOperation.isSingleton());
    }

    /**
     * Create an {@link Iterable<ConsumeMessage>} containing 10 messages. The given {@param start} value will be
     * append to the body and key of the message
     *
     * @param start
     *         value to be append to the messages
     * @return an Iterable of ConsumeMessages
     */
    private static Iterable<ConsumeMessage> records(int start) {
        List<ConsumeMessage> messages = new ArrayList<>(10);
        for (int i = start; i < (start + 10); i++) {
            messages.add(new ConsumeMessage(new ConsumerRecord<>("topic", 0, i, "key" + i, toStream("value" + i))));
        }

        return messages;
    }

    /**
     * Create an on-memory {@link InputStream} containing the given {@link String} {@param value}
     *
     * @param value
     *         to be contained by the {@link InputStream}
     * @return the inputStream
     */
    private static InputStream toStream(String value) {
        return new FastByteArrayInputStream(value.getBytes(StringUtil.UTF8_CHARSET));
    }

    /**
     * Create a {@link Listener} that will return the given batches when a call to {@link Listener#getBatch()} is made.
     * The listener will return a new {@link PayloadBatchDouble} if it didn't return any batch yet or if the
     * previously returned batch is marked as submitted.
     *
     * @param batches
     *         the batches to be returned by the listener
     * @return the listener mock
     */
    private static Listener createListenerMock(final PayloadBatchDouble... batches) {
        final AtomicInteger position = new AtomicInteger(0);

        Listener listener = Mockito.mock(Listener.class);
        when(listener.getBatch()).thenAnswer(new Answer<PayloadBatch>() {
            @Override
            public PayloadBatch answer(InvocationOnMock invocation) {
                PayloadBatchDouble batch = batches[position.get()];
                if (!batch._isSubmitted) {
                    return batch;
                }

                return batches[position.incrementAndGet()];
            }
        });

        return listener;
    }

    /**
     * Create a mock for {@link BoomiConsumer} that returns batches of ten messages when calling to
     * {@link BoomiConsumer#poll}.
     *
     * When calling to {@link BoomiListenerConsumer#rewind}, the next call to {@link BoomiConsumer#poll} returns the
     * same
     * batch of messages that was returned in the previous call.
     *
     * @return the consumer mock
     */
    @SuppressWarnings("unchecked")
    private static BoomiListenerConsumer mockConsumer() {
        BoomiListenerConsumer consumer = Mockito.mock(BoomiListenerConsumer.class, Mockito.RETURNS_DEFAULTS);

        // this consumer will return a batch of 10 messages after 1 second
        final AtomicInteger startIndex = new AtomicInteger(-1);
        when(consumer.poll()).thenAnswer(new Answer<Iterable<ConsumeMessage>>() {
            @Override
            public Iterable<ConsumeMessage> answer(InvocationOnMock invocation) {
                return records(startIndex.incrementAndGet() * 10);
            }
        });

        // rewind to previous batch
        Mockito.doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                startIndex.decrementAndGet();
                return null;
            }
        }).when(consumer).rewind(Mockito.anyMap());

        return consumer;
    }

    @SuppressWarnings("unchecked")
    private static class PayloadBatchDouble implements PayloadBatch {

        Throwable _error;
        List<Payload> _payloads = new ArrayList<>();
        boolean _isSubmitted = false;
        Future<ListenerExecutionResult> _result;

        PayloadBatchDouble() throws ExecutionException, InterruptedException {
            this(true, true);
        }

        PayloadBatchDouble(boolean shouldFinish) throws ExecutionException, InterruptedException {
            this(shouldFinish, true);
        }

        PayloadBatchDouble(boolean shouldFinish, boolean isSuccess) throws ExecutionException, InterruptedException {
            ListenerExecutionResult executionResult = Mockito.mock(ListenerExecutionResult.class);
            when(executionResult.isSuccess()).thenReturn(isSuccess);

            Future<ListenerExecutionResult> result = Mockito.mock(Future.class);
            when(result.isDone()).thenReturn(shouldFinish);
            when(result.get()).thenReturn(executionResult);

            _result = result;
        }

        @Override
        public void add(Payload payload) {
            _payloads.add(payload);
        }

        @Override
        public void submit() {
            Assert.fail();
        }

        @Override
        public void submit(Throwable error) {
            _isSubmitted = true;
            _error = error;
        }

        @Override
        public Future<ListenerExecutionResult> submit(SubmitOptions options) {
            _isSubmitted = true;
            return _result;
        }

        @Override
        public int getCount() {
            return _payloads.size();
        }

        @Override
        public long getSize() {
            return _payloads.size();
        }
    }

    private static class ConnectionDouble extends KafkaPollingConnection {

        private String _topic;
        private BoomiListenerConsumer _consumer;

        ConnectionDouble(KafkaITContext context, BoomiListenerConsumer consumer, String topic) {
            super(context);
            _consumer = consumer;
            _topic = topic;
        }

        @Override
        public String getObjectTypeId() {
            return _topic;
        }

        @Override
        BoomiListenerConsumer createPollingConsumer(String topic) {
            assertEquals(_topic, topic);
            return _consumer;
        }

        @Override
        public KafkaITContext getContext() {
            return (KafkaITContext) super.getContext();
        }
    }
}
