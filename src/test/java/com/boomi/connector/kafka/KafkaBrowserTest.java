
package com.boomi.connector.kafka;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.CollectionUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@RunWith(Theories.class)
public class KafkaBrowserTest {

    private static final List<ObjectDefinitionRole> ROLES = Arrays.asList(ObjectDefinitionRole.INPUT,
            ObjectDefinitionRole.OUTPUT);

    @DataPoints(value = "dynamic topic contexts")
    public static Collection<KafkaITContext> dynamicTopicContexts() {
        Collection<KafkaITContext> contexts = new ArrayList<>();

        // Produce
        KafkaITContext produceContext = KafkaITContext.produceVM();
        produceContext.addOperationProperty(Constants.KEY_ALLOW_DYNAMIC_TOPIC, true);

        contexts.add(produceContext);

        // Commit Offset
        KafkaITContext commitOffset = KafkaITContext.commitOffsetVM();
        commitOffset.addOperationProperty(Constants.KEY_ALLOW_DYNAMIC_TOPIC, true);

        contexts.add(commitOffset);

        return contexts;
    }

    @Theory
    public void getObjectTypesForDynamicTopicsTest(
            @FromDataPoints("dynamic topic contexts") KafkaITContext context) {
        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);
        KafkaBrowser browser = new KafkaBrowser(connection);

        ObjectTypes objectTypes = browser.getObjectTypes();

        Assert.assertNotNull(objectTypes);

        List<ObjectType> types = objectTypes.getTypes();

        Assert.assertNotNull(types);
        Assert.assertEquals(1, types.size());

        ObjectType type = CollectionUtil.getFirst(types);

        Assert.assertNotNull(type);
        Assert.assertEquals(Constants.DYNAMIC_TOPIC_LABEL, type.getLabel());
        Assert.assertEquals(Constants.DYNAMIC_TOPIC_ID, type.getId());
    }

    @Test
    public void getObjectDefinitionsForProduceTest() {
        KafkaITContext context = KafkaITContext.produceVM();

        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);
        KafkaBrowser browser = new KafkaBrowser(connection);

        ObjectDefinitions definitions = browser.getObjectDefinitions("topic", ROLES);

        boolean hasBinaryInput = false;
        boolean hasNoneInput = false;
        for (ObjectDefinition definition : definitions.getDefinitions()) {
            Assert.assertEquals(ContentType.NONE, definition.getOutputType());

            hasBinaryInput |= ContentType.BINARY == definition.getInputType();
            hasNoneInput |= ContentType.NONE == definition.getInputType();
        }

        Assert.assertTrue("must have a binary input", hasBinaryInput);
        Assert.assertTrue("must have a none input", hasNoneInput);
    }

    @Test
    public void getObjectDefinitionsForConsumeTest() {
        KafkaITContext context = KafkaITContext.consumeVM();

        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);
        KafkaBrowser browser = new KafkaBrowser(connection);

        ObjectDefinitions definitions = browser.getObjectDefinitions("topic", ROLES);

        boolean hasBinaryOutput = false;
        boolean hasNoneOutput = false;
        for (ObjectDefinition definition : definitions.getDefinitions()) {
            Assert.assertEquals(ContentType.NONE, definition.getInputType());

            hasBinaryOutput |= ContentType.BINARY == definition.getOutputType();
            hasNoneOutput |= ContentType.NONE == definition.getOutputType();
        }

        Assert.assertTrue("must have none output", hasNoneOutput);
        Assert.assertTrue("must have binary output", hasBinaryOutput);
    }

    @Test
    public void getObjectDefinitionsForCommitOffsetTest() {
        KafkaITContext context = KafkaITContext.commitOffsetVM();

        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);
        KafkaBrowser browser = new KafkaBrowser(connection);

        ObjectDefinitions definitions = browser.getObjectDefinitions("topic", ROLES);

        boolean hasNoneInput = false;
        boolean hasJsonInput = false;

        for (ObjectDefinition definition : definitions.getDefinitions()) {
            Assert.assertEquals(ContentType.NONE, definition.getOutputType());

            boolean isInput = ContentType.NONE != definition.getInputType();

            if (isInput) {
                Assert.assertNotNull(definition.getJsonSchema());
            }

            hasJsonInput |= ContentType.JSON == definition.getInputType();
            hasNoneInput |= ContentType.NONE == definition.getInputType();
        }

        Assert.assertTrue("must have json input", hasJsonInput);
        Assert.assertTrue("must have none input", hasNoneInput);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getObjectDefinitionForInvalidOperationTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setOperationCustomType(CustomOperationType.TEST_CONNECTION.name());

        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);
        KafkaBrowser browser = new KafkaBrowser(connection);

        browser.getObjectDefinitions("topic", ROLES);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getObjectTypesForInvalidOperationTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setOperationCustomType(CustomOperationType.TEST_CONNECTION.name());

        KafkaConnection<BrowseContext> connection = TestUtils.createConnection(context);
        KafkaBrowser browser = new KafkaBrowser(connection);

        browser.getObjectTypes();
    }

    @Test(expected = ConnectorException.class)
    public void testConnectionFailTest() {
        KafkaITContext context = KafkaITContext.produceVM();
        context.setOperationCustomType(CustomOperationType.TEST_CONNECTION.name());

        KafkaConnection<BrowseContext> connectionDouble = new KafkaConnection<BrowseContext>(context) {
            @Override
            public Set<String> getTopics() {
                throw new ConnectorException("fail!");
            }
        };

        ConnectionTester browser = new KafkaBrowser(connectionDouble);
        browser.testConnection();
    }
}
