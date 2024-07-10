
package com.boomi.connector.kafka;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.kafka.operation.CustomOperationType;
import com.boomi.connector.kafka.util.Constants;
import com.boomi.util.CollectionUtil;

import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(Theories.class)
public class KafkaBrowserIT {

    @DataPoints(value = "contexts")
    public static Collection<KafkaITContext> getContexts() {
        Set<KafkaITContext> contexts = new HashSet<>(2);

        // Produce
        KafkaITContext produceContext = KafkaITContext.addVMConnectionProperties(new KafkaITContext());
        produceContext.setOperationCustomType(CustomOperationType.PRODUCE.name());
        produceContext.setOperationType(OperationType.EXECUTE);
        produceContext.addOperationProperty(Constants.KEY_ALLOW_DYNAMIC_TOPIC, false);

        contexts.add(produceContext);

        // Consume
        KafkaITContext consumeContext = KafkaITContext.addVMConnectionProperties(new KafkaITContext());
        consumeContext.setOperationCustomType(CustomOperationType.CONSUME.name());
        consumeContext.setOperationType(OperationType.EXECUTE);

        contexts.add(consumeContext);

        // Commit Offset
        KafkaITContext commitOffsetContext = KafkaITContext.addVMConnectionProperties(new KafkaITContext());
        commitOffsetContext.setOperationCustomType(CustomOperationType.COMMIT_OFFSET.name());
        commitOffsetContext.setOperationType(OperationType.EXECUTE);
        commitOffsetContext.addOperationProperty(Constants.KEY_ALLOW_DYNAMIC_TOPIC, false);

        contexts.add(commitOffsetContext);

        return contexts;
    }

    @Theory
    public void getObjectTypesTest(@FromDataPoints("contexts") BrowseContext context) {
        KafkaConnection<BrowseContext> connection = new KafkaConnection<>(context);

        KafkaBrowser browser = new KafkaBrowser(connection);

        ObjectTypes objectTypes = browser.getObjectTypes();

        Assert.assertNotNull(objectTypes);

        List<ObjectType> types = objectTypes.getTypes();
        Assert.assertFalse(CollectionUtil.isEmpty(types));
        for (ObjectType type : types) {
            boolean isDynamicObject = Constants.DYNAMIC_TOPIC_LABEL.equals(type.getLabel());
            isDynamicObject &= Constants.DYNAMIC_TOPIC_ID.equals(type.getId());

            Assert.assertFalse("Dynamic Object Type should not be present", isDynamicObject);
        }
    }
}
