package com.boomi.connector.kafka.operation.polling;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.listen.ListenManager;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.SingletonListenOperation;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;
import com.boomi.connector.kafka.operation.polling.execution.ProcessExecution;
import com.boomi.connector.kafka.util.TopicNameUtil;
import com.boomi.connector.util.listen.UnmanagedListenOperation;
import com.boomi.util.ExecutorUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.RebalanceInProgressException;

import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Listen Operation for retrieving messages from Apache Kafka.
 *
 * The operation asks for messages in the defined topic to the broker and submits a process execution every time the
 * method {@link KafkaPollingOperation#run} is invoked.
 */
public class KafkaPollingOperation extends UnmanagedListenOperation
        implements Runnable, SingletonListenOperation<ListenManager> {

    private static final Logger LOG = LogUtil.getLogger(KafkaPollingOperation.class);

    private static final TimeUnit SHUTDOWN_WAIT_UNIT = TimeUnit.MINUTES;
    private static final long SHUTDOWN_WAIT_TIME = 1L;

    // this object is used as a lock by the synchronized methods of this class
    private final BoomiListenerConsumer _consumer;

    private final String _topic;
    private final KafkaPhasedRetry _retry;
    private final long _delay;
    private final long _interval;
    private final ScheduledExecutorService _executor;
    private final boolean _isSingletonListener;

    private Listener _listener;
    private ProcessExecution _processExecution;

    public KafkaPollingOperation(KafkaPollingConnection connection) {
        this(connection, getExecutorServiceName(connection));
    }

    private KafkaPollingOperation(KafkaPollingConnection connection, String operationId) {
        this(connection, connection.createPollingConsumer(
                        TopicNameUtil.getTopic(connection.getObjectTypeId(), connection.getContext())),
                ExecutorUtil.newScheduler(operationId));
    }

    // for unit tests
    KafkaPollingOperation(KafkaPollingConnection connection, BoomiListenerConsumer consumer,
                          ScheduledExecutorService executor) {
        super(connection);
        _topic = TopicNameUtil.getTopic(connection.getObjectTypeId(), connection.getContext());
        _consumer = consumer;
        _retry = new KafkaPhasedRetry();
        _executor = executor;
        _delay = connection.getPollingDelay();
        _interval = connection.getPollingInterval();
        _isSingletonListener = connection.isSingletonListener();
    }

    private static String getExecutorServiceName(KafkaPollingConnection connection) {
        String topic = TopicNameUtil.getTopic(connection.getObjectTypeId(), connection.getContext());
        String clientId = connection.getClientId();
        String consumerGroup = connection.getConsumerGroup();

        return String.format("%s:%s:%s:%s", clientId, consumerGroup, topic, UUID.randomUUID());
    }

    /**
     * Start the operation. An {@link ScheduledExecutorService} is started to poll the topic at regular intervals
     * configured by the user
     *
     * @param listener
     *         the listener where message will be submitted
     */
    @Override
    public final void start(Listener listener) {
        synchronized (_consumer) {
            boolean isSuccess = false;
            try {
                _consumer.subscribe();
                _listener = listener;
                _processExecution = null;
                isSuccess = true;
            } finally {
                if (!isSuccess) {
                    IOUtil.closeQuietly(_consumer);
                }
            }
        }

        _executor.scheduleAtFixedRate(this, _delay, _interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the operation and free the resources used by this instance.
     */
    @Override
    public void stop() {
        synchronized (_consumer) {
            _listener = null;
            _processExecution = null;

            try {
                ExecutorUtil.shutdownQuietly(_executor, SHUTDOWN_WAIT_TIME, SHUTDOWN_WAIT_UNIT);
            } finally {
                IOUtil.closeQuietly(_consumer);
            }
        }
    }

    /**
     * Polls for a batch of messages from the configured topic and submits them to the {@link Listener} provided on
     * {@link KafkaPollingOperation#start(Listener)}.
     */
    @Override
    public void run() {
        synchronized (_consumer) {
            if ((_listener == null) || !isConsumerReady()) {
                // the listener was not started, the consumer is not connected or the consumer-group is being rebalanced
                // this cycle will be skipped
                return;
            }

            try {
                if (isExecutionPending()) {
                    // Only one concurrent process is permitted, will skip polling until the current execution is
                    // completed
                    return;
                }

                for (ConsumeMessage message : _consumer.poll()) {
                    if (_processExecution == null) {
                        _processExecution = new ProcessExecution(_listener.getBatch(), getContext(),
                                _isSingletonListener);
                    }
                    _processExecution.add(message);
                }

                if (_processExecution != null) {
                    // All the messages were included without any error nor re-balance the batch can be submitted
                    _processExecution.submit();
                }
            } catch (RebalanceInProgressException e) {
                //Log and continue to retry in the next cycle. No need to trigger a failed process execution
                LOG.log(Level.INFO, e.getMessage(), e);
            } catch (IllegalStateException | KafkaException e) {
                // An illegalStateException means that the consumer is closed or wasn't subscribed to a topic.
                // A KafkaException may indicate an unrecoverable error on the consumer. A failed execution will be
                // submitted to notify the problem and a the consumer will be closed in order to attempt a
                // re-subscription in the next iteration.
                LOG.log(Level.WARNING, "The service returned an error. Reconnecting", e);
                submitFailedExecution(e);
                _consumer.close();
            } catch (Exception e) {
                LOG.log(Level.WARNING, e, () -> "Failed polling from topic: " + _topic);
                submitFailedExecution(e);
            }
        }
    }

    /**
     * Evaluates if there is an execution already running from a previous call to {@link KafkaPollingOperation#run()}.
     * In case there is an existing execution:
     * <ul>
     * <li>if it finished successfully, the offset are committed and {@code false} is returned</li>
     * <li>if it failed, the offset are rewind and {@code false} is returned</li>
     * <li>if it is still processing, {@code true} is returned</li>
     * </ul>
     *
     * If there isn't any previous execution submitted, {@code false} is returned.
     *
     * @return {@code true} if a pending execution is being processed, {@code false} otherwise
     */
    private boolean isExecutionPending() {
        if (_processExecution == null) {
            return false;
        }

        ProcessExecution.ExecutionState state = _processExecution.getState();
        switch (state) {
            case PROCESSING:
                // there is a process execution running
                return true;
            case SUCCESS:
                // the process execution finished successfully commit the successfully processed offsets
                _consumer.commit(_processExecution.endOffsets());
                break;
            case FAILED:
            case DISCARDED:
                // the process execution finished with errors or was discarded
                // rewind the offsets
                _consumer.rewind(_processExecution.startOffsets());
                break;
            default:
                // the process execution has an invalid state, discard it
                _processExecution = null;
                throw new ConnectorException("The current execution state is not valid: " + state);
        }

        _processExecution = null;
        return false;
    }

    /**
     * Forces a failed execution entry in process reporting by getting a new batch and submitting it with an exception
     */
    private void submitFailedExecution(Throwable t) {
        if (_processExecution != null) {
            _processExecution.fail(t);
        } else {
            _listener.getBatch().submit(t);
        }
    }

    /**
     * Verifies if the consumer is subscribed to a topic or if partitions were manually assigned. If not (because the
     * connection was lost), attempt to resubscribe it
     *
     * @return {@code true} if the consumer is subscribed, {@code false} otherwise
     */
    private boolean isConsumerReady() {
        if (_consumer.isSubscribed()) {
            return true;
        }

        if (_retry.canRetry()) {
            try {
                LOG.log(Level.INFO, "Attempting to re-subscribe to topic {0}", _topic);
                _consumer.subscribe();
                _retry.reset();

                // consumer resubscribed successfully, check if it is ready and return accordingly
                return _consumer.isSubscribed();
            } catch (Exception e) {
                LOG.log(Level.WARNING, e, () -> "failed to resubscribe to topic " + _topic);
                _retry.increment();
            }
        }

        // the resubscribed failed or the backoff time was not reached yet
        return false;
    }

    /**
     * Accessor for the singleton listener property. This value indicates whether the listener should be started in all
     * the nodes of a multi-node environment (i.e molecule) or if it should run in only one of the nodes.
     *
     * @return the singleton listener mode
     */
    @Override
    public boolean isSingleton() {
        return _isSingletonListener;
    }

    /**
     * Helper class to orchestrate a Phased Retry Strategy without blocking the execution.
     *
     * The {@link #canRetry()} method indicates if it's time to attempt a retry or if it's necessary to keep waiting.
     *
     * Depending on how many times the method {@link #increment()} is called, {@link #canRetry()} will return true after
     * 0, 10, 30, 60, 120 or 300 seconds.
     *
     * A {@link #reset()} method is exposed to allow an instance of this class to be reused.
     */
    private static class KafkaPhasedRetry {

        // these values were extracted from PhasedRetry
        private static final int[] DELAYS = new int[] { 0, 10_000, 30_000, 60_000, 120_000, 300_000 };

        private long _nextRetryTime;
        private int _retries;

        /**
         * Verifies if it is time to attempt a retry
         *
         * @return true if a retry should be attempted or false if it necessary to keep waiting
         */
        boolean canRetry() {
            return (System.currentTimeMillis() > _nextRetryTime);
        }

        void reset() {
            _retries = 0;
            _nextRetryTime = 0L;
        }

        void increment() {
            _retries++;
            _nextRetryTime = System.currentTimeMillis() + getBackoff();
        }

        private int getBackoff() {
            int index = Math.min(_retries, (DELAYS.length - 1));
            return DELAYS[index];
        }
    }
}