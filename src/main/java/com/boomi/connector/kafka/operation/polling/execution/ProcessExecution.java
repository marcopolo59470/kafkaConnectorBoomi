// Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.kafka.operation.polling.execution;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PayloadMetadataFactory;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.api.listen.PayloadBatch;
import com.boomi.connector.api.listen.SubmitOptions;
import com.boomi.connector.api.listen.options.DistributionMode;
import com.boomi.connector.api.listen.options.WaitMode;
import com.boomi.connector.kafka.client.consumer.ConsumeMessage;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 */
public class ProcessExecution {

    private final Map<TopicPartition, OffsetAndMetadata> _endOffsets = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> _startOffsets = new HashMap<>();
    private final PayloadBatch _batch;
    private final PayloadMetadataFactory _payloadMetadataFactory;
    private final boolean _isSingletonListener;

    private Future<ListenerExecutionResult> _currentExecution;
    private ExecutionState _state = ExecutionState.NONE;

    public ProcessExecution(PayloadBatch batch, PayloadMetadataFactory payloadMetadataFactory,
            boolean isSingletonListener) {
        _batch = batch;
        _payloadMetadataFactory = payloadMetadataFactory;
        _isSingletonListener = isSingletonListener;
    }

    /**
     * Evaluates the state of the process execution and return the corresponding {@link ExecutionState}
     *
     * @return the state of the process execution
     */
    public ExecutionState getState() {
        //the ExecutionState will be PROCESSING only if the batch was submitted and _currentExecution is not null.
        if ((_state == ExecutionState.PROCESSING) && _currentExecution.isDone()) {
            _state = getExecutionResult();
        }
        return _state;
    }

    private ExecutionState getExecutionResult() {
        try {
            ListenerExecutionResult result = _currentExecution.get();
            return result.isSuccess() ? ExecutionState.SUCCESS : ExecutionState.FAILED;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectorException(e);
        } catch (ExecutionException e) {
            throw new ConnectorException("Error getting process execution result", e);
        }
    }

    /**
     * Add the given message to the batch
     *
     * @param message
     *         the message to be added
     */
    public void add(ConsumeMessage message) {
        validateExecution();

        _batch.add(message.toPayload(_payloadMetadataFactory.createMetadata()));

        TopicPartition partition = message.getTopicPartition();
        // keep track of the *last* offset per partition
        _endOffsets.put(partition, message.getNextOffset());

        // keep track of the *first* offset for each partition
        _startOffsets.computeIfAbsent(partition, key -> message.getOffset());
    }

    /**
     * Submit the messages previously added to this instance
     */
    public void submit() {
        validateExecution();

        try {
            SubmitOptions options = new SubmitOptions().withWaitMode(WaitMode.PROCESS_COMPLETION);

            if (_isSingletonListener) {
                options.withDistributionMode(DistributionMode.PREFER_REMOTE);
            }

            _currentExecution = _batch.submit(options);
            _state = ExecutionState.PROCESSING;
        } catch (IllegalStateException e) {
            // wrapping IllegalStateException because it clashes with the exception thrown by KafkaConsumer to signal
            // an invalid state of the client
            throw new ConnectorException(e);
        }
    }

    public void fail(Throwable t) {
        if (_state != ExecutionState.NONE) {
            return;
        }

        try {
            _state = ExecutionState.DISCARDED;
            _batch.submit(t);
        } catch (IllegalStateException e) {
            // wrapping IllegalStateException because it clashes with the exception thrown by KafkaConsumer to signal
            // an invalid state of the client
            throw new ConnectorException(e);
        }
    }

    private void validateExecution() {
        if (_state != ExecutionState.NONE) {
            throw new ConnectorException("This execution has already been submitted");
        }
    }

    /**
     * Get the latest offset position per partition added to this {@link ProcessExecution}
     */
    public Map<TopicPartition, OffsetAndMetadata> endOffsets() {
        return Collections.unmodifiableMap(_endOffsets);
    }

    /**
     * Get the earliest offset position per partition added to this {@link ProcessExecution}
     */
    public Map<TopicPartition, OffsetAndMetadata> startOffsets() {
        return Collections.unmodifiableMap(_startOffsets);
    }

    /**
     * Enumeration that represents the state of the process execution associated to an instance of
     * {@link ProcessExecution}
     * <ol>
     * <li>{@code NONE}: there isn't any process execution being running at the moment</li>
     * <li>{@code PROCESSING}: a process execution is running</li>
     * <li>{@code SUCCESS}: a process execution finished successfully</li>
     * <li>{@code FAILED}: the process execution finished with errors</li>
     * <li>{@code DISCARDED}: the process execution was discarded</li>
     * </ol>
     */
    public enum ExecutionState {
        NONE, PROCESSING, SUCCESS, FAILED, DISCARDED
    }
}
