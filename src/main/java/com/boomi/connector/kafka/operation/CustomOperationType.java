
package com.boomi.connector.kafka.operation;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;

/**
 * Operation Type identifiers
 */
public enum CustomOperationType {
    TEST_CONNECTION,
    PRODUCE,
    CONSUME,
    COMMIT_OFFSET,
    LISTEN;

    /**
     * Extract the {@link CustomOperationType} from the given {@link BrowseContext}
     *
     * @param context
     *         to get the {@link CustomOperationType}
     * @return the {@link CustomOperationType}
     * @throws ConnectorException
     *         if an the type set in the context is not valid
     */
    public static CustomOperationType fromContext(BrowseContext context) {
        String operationType = context.getCustomOperationType();
        try {
            return valueOf(operationType);
        } catch (IllegalArgumentException e) {
            throw new ConnectorException("Invalid custom operation type: " + operationType);
        }
    }
}
