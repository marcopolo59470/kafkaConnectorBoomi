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
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.json.JSONUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Browser implementation that includes a testConnection method that asserts the connection with Kafka performing a
 * request to retrieve the available Topics
 */
public class KafkaBrowser extends BaseBrowser implements ConnectionTester {

    private static final String ERROR_SCHEMA_RESOURCE_NOT_FOUND = "The %s schema file could not be loaded.";
    private static final String SCHEMA_COMMIT_OFFSET_INPUT_WITH_TOPIC = "/schemas/commit_offset_input_with_topic.json";
    private static final String SCHEMA_COMMIT_OFFSET_INPUT_WITHOUT_TOPIC =
            "/schemas/commit_offset_input_without_topic.json";
    private static final ObjectTypes OBJECT_TYPE_DYNAMIC_TOPIC = new ObjectTypes().withTypes(
            new ObjectType().withId(Constants.DYNAMIC_TOPIC_ID).withLabel(Constants.DYNAMIC_TOPIC_LABEL));

    private static final Comparator<ObjectType> OBJECT_TYPE_COMPARATOR = new Comparator<ObjectType>() {
        @Override
        public int compare(ObjectType o1, ObjectType o2) {
            return o1.getLabel().compareTo(o2.getLabel());
        }
    };

    KafkaBrowser(KafkaConnection<BrowseContext> conn) {
        super(conn);
    }

    private static boolean isInput(ObjectDefinitionRole role) {
        return (ObjectDefinitionRole.INPUT == role);
    }

    private static boolean isOutput(ObjectDefinitionRole role) {
        return (ObjectDefinitionRole.OUTPUT == role);
    }

    private static void produceDefinitions(Collection<ObjectDefinitionRole> roles,
            Collection<ObjectDefinition> definitions) {
        for (ObjectDefinitionRole role : roles) {
            definitions.add(new ObjectDefinition().withInputType(isInput(role) ? ContentType.BINARY : ContentType.NONE)
                    .withOutputType(ContentType.NONE));
        }
    }

    private static void consumeDefinitions(Collection<ObjectDefinitionRole> roles,
            Collection<ObjectDefinition> definitions) {
        for (ObjectDefinitionRole role : roles) {
            definitions.add(new ObjectDefinition().withInputType(ContentType.NONE)
                    .withOutputType(isOutput(role) ? ContentType.BINARY : ContentType.NONE));
        }
    }

    private static void commitOffsetDefinitions(Collection<ObjectDefinitionRole> roles,
            Collection<ObjectDefinition> definitions, String objectTypeId) {
        String schema = Constants.DYNAMIC_TOPIC_ID.equals(objectTypeId) ? SCHEMA_COMMIT_OFFSET_INPUT_WITH_TOPIC
                : SCHEMA_COMMIT_OFFSET_INPUT_WITHOUT_TOPIC;

        for (ObjectDefinitionRole role : roles) {
            ObjectDefinition definition = new ObjectDefinition().withOutputType(ContentType.NONE);
            if (isInput(role)) {
                definition.withInputType(ContentType.JSON).withJsonSchema(load(schema)).withElementName(
                        JSONUtil.FULL_DOCUMENT_JSON_POINTER);
            } else {
                definition.withInputType(ContentType.NONE);
            }
            definitions.add(definition);
        }
    }

    private static String load(String resource) {
        try {
            return JSONUtil.loadSchemaFromResource(resource).toString();
        } catch (IOException e) {
            throw new ConnectorException(String.format(ERROR_SCHEMA_RESOURCE_NOT_FOUND, resource), e);
        }
    }

    @Override
    public ObjectTypes getObjectTypes() {
        CustomOperationType operationType = getOperationType();

        switch (operationType) {
            case PRODUCE:
            case COMMIT_OFFSET:
            case CONSUME:
            case LISTEN:
                return isDynamicTopic() ? OBJECT_TYPE_DYNAMIC_TOPIC : getTypesFromService();
            default:
                throw new UnsupportedOperationException();
        }
    }

    private CustomOperationType getOperationType() {
        return CustomOperationType.fromContext(getContext());
    }

    private boolean isDynamicTopic() {
        return getContext().getOperationProperties().getBooleanProperty(
                Constants.KEY_ALLOW_DYNAMIC_TOPIC, false);
    }


    /**
     * Builds and fills an {@link ObjectTypes} with all the Topic fetch from Kafka. The topics are ordered
     * alphabetically.
     *
     * @return an {@link ObjectTypes} with the Topics
     */
    private ObjectTypes getTypesFromService() {
        Set<String> topics = getConnection().getTopics();
        Set<ObjectType> types = new TreeSet<>(OBJECT_TYPE_COMPARATOR);

        for (String topic : topics) {
            types.add(new ObjectType().withId(topic).withLabel(topic));
        }

        return new ObjectTypes().withTypes(types);
    }

    @Override
    public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
        CustomOperationType operationType = getOperationType();
        ObjectDefinitions objectDefinitions = new ObjectDefinitions();
        Collection<ObjectDefinition> definitions = new ArrayList<>(roles.size());

        switch (operationType) {
            case PRODUCE:
                produceDefinitions(roles, definitions);
                break;
            case CONSUME:
            case LISTEN:
                consumeDefinitions(roles, definitions);
                break;
            case COMMIT_OFFSET:
                commitOffsetDefinitions(roles, definitions, objectTypeId);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        if (isDynamicTopic()) {
            objectDefinitions.withOperationFields(buildImportableTopicField(
                    getContext().getOperationProperties()
                            .getProperty(Constants.TOPIC_NAME_FIELD_ID)));
        }
        return objectDefinitions.withDefinitions(definitions);
    }

    private static BrowseField buildImportableTopicField(String defaultTopicName) {
        return new BrowseField()
                .withId(Constants.TOPIC_NAME_FIELD_ID)
                .withLabel(Constants.TOPIC_NAME_LABEL)
                .withType(DataType.STRING)
                .withHelpText(Constants.TOPIC_NAME_HELP_TEXT)
                .withOverrideable(true)
                .withDefaultValue(defaultTopicName);
    }

    /**
     * Perform a request to Kafka in order to retrieve a {@link Set} of the available topics
     *
     * @throws ConnectorException
     *         if any error happens while performing the request
     */
    @Override
    public void testConnection() {
        try {
            getConnection().getTopics();
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    public KafkaConnection<BrowseContext> getConnection() {
        return (KafkaConnection<BrowseContext>) super.getConnection();
    }
    
}