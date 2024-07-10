// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.kafka.util;

import com.boomi.connector.kafka.KafkaITContext;
import com.boomi.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author ashutoshjadhav.
 */
public class TopicNameUtilTest {

    public static final String TEST_TOPIC = "test_topic";
    public static final String DYNAMIC_TOPIC = "dynamic_topic";

    @Test
    public void testTopicWithoutDynamicTopic() {
        KafkaITContext context = KafkaITContext.consumeVM();

        String result = TopicNameUtil.getTopic(TEST_TOPIC, context);

        Assert.assertEquals(TEST_TOPIC, result);
    }

    @Test
    public void testDynamicTopicOverrideWithConsume() {
        String objectTypeId = Constants.DYNAMIC_TOPIC_ID;
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, DYNAMIC_TOPIC);

        String result = TopicNameUtil.getTopic(objectTypeId, context);

        Assert.assertEquals(DYNAMIC_TOPIC, result);

    }

    @Test
    public void testEmptyDynamicTopicOverrideWithConsume() {
        String objectTypeId = Constants.DYNAMIC_TOPIC_ID;
        KafkaITContext context = KafkaITContext.consumeVM();
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, "");

        String result = TopicNameUtil.getTopic(objectTypeId, context);

        Assert.assertTrue(StringUtil.isBlank(result));
    }

    @Test
    public void testDynamicTopicOverrideWithProduce() {
        String objectTypeId = Constants.DYNAMIC_TOPIC_ID;
        KafkaITContext context = KafkaITContext.produceVM();
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, DYNAMIC_TOPIC);

        String result = TopicNameUtil.getTopic(objectTypeId, context);

        Assert.assertEquals(DYNAMIC_TOPIC, result);
    }

    @Test
    public void testEmptyDynamicTopicOverrideWithProduce() {
        String objectTypeId = Constants.DYNAMIC_TOPIC_ID;
        KafkaITContext context = KafkaITContext.produceVM();
        context.addOperationProperty(Constants.TOPIC_NAME_FIELD_ID, "");

        String result = TopicNameUtil.getTopic(objectTypeId, context);

        Assert.assertEquals(Constants.DYNAMIC_TOPIC_ID, result);
    }

}
