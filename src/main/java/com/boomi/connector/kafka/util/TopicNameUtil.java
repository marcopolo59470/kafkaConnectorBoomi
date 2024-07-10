// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.kafka.util;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.util.StringUtil;

/**
 * @author ashutoshjadhav.
 */
public final class TopicNameUtil {
    private TopicNameUtil() {
    }

    /**
     * The operation will use the topic in the operation ID or if it's dynamic will use the
     * dynamic topic field value or override topic set in document property based on the type
     * of operation.
     *
     * @param operationId objectTypeId
     * @param context     BrowseContext
     * @return topic name
     */
    public static String getTopic(String operationId, BrowseContext context) {

        String topicName = Constants.DYNAMIC_TOPIC_ID.equals(operationId) ?
                context.getOperationProperties().getProperty(Constants.TOPIC_NAME_FIELD_ID) : operationId;

        CustomOperationType operationType = CustomOperationType.fromContext(context);
        switch (operationType) {
            case PRODUCE:
            case COMMIT_OFFSET:
                return StringUtil.isBlank(topicName) ? Constants.DYNAMIC_TOPIC_ID : topicName;
            case CONSUME:
            case LISTEN:
                return topicName;
            default:
                throw new UnsupportedOperationException();
        }
    }

}
