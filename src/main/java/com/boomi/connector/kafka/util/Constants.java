
package com.boomi.connector.kafka.util;

import com.boomi.util.ByteUnit;

public final class Constants {

    // connection properties
    public static final String KEY_USERNAME = "username";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_SERVERS = "bootstrap_servers";
    public static final String KEY_SECURITY_PROTOCOL = "security_protocol";
    public static final String KEY_SASL_MECHANISM = "sasl_mechanism";
    public static final String KEY_OAUTH_TOKEN_URL = "oauth_token_url";
    public static final String KEY_OAUTH_CLIENT_ID = "oauth_client_id";
    public static final String KEY_BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static final String KEY_OAUTH_CLIENT_SECRET = "oauth_client_secret";
    public static final String KEY_OAUTH_SCOPE = "oauth_scope";
    public static final String KEY_CERTIFICATE = "private_certificate";
    public static final String KEY_SERVICE_PRINCIPAL = "service_principal";

    // container properties
    public static final String KEY_MAX_FETCH_SIZE = "com.boomi.connector.kafka.max.request.size";
    public static final String KEY_MAX_POLL_RECORDS = "com.boomi.connector.kafka.max.poll.records";
    public static final int DEFAULT_MAX_POLL_RECORDS = 1;

    // The default max fetch size is set to 10 MB to mitigate the possibility of triggering a misconfiguration issue
    // if this value is bigger on the broker.
    public static final int DEFAULT_MAX_FETCH_SIZE = (int) ByteUnit.byteSize(10, ByteUnit.MB.name());

    // operations
    public static final String KEY_CLIENT_ID = "client_id";

    // producer
    public static final String KEY_COMPRESSION_TYPE = "compression_type";
    public static final String KEY_ACKS = "acks";
    public static final String KEY_MAXIMUM_TIME_TO_WAIT = "operation_timeout";
    public static final String KEY_ALLOW_DYNAMIC_TOPIC = "allow_dynamic_topics";

    // consumer & commit offset
    public static final String KEY_CONSUMER_GROUP = "consumer_group";

    // consumer
    public static final String KEY_RECEIVE_MESSAGE_TIMEOUT = "max_wait_timeout";
    public static final String KEY_MIN_MESSAGES = "min_messages";
    public static final String KEY_AUTOCOMMIT = "autocommit";

    // consumer & listen
    public static final String KEY_AUTO_OFFSET_RESET = "auto_offset_reset";

    // listen
    public static final String KEY_POLLING_INTERVAL = "polling_interval";
    public static final String KEY_MAX_MESSAGES = "max_messages_poll";
    public static final String KEY_POLLING_DELAY = "polling_delay";
    public static final String KEY_SINGLETON_LISTENER = "is_singleton";
    public static final String KEY_ASSIGN_PARTITIONS = "assign_partitions";
    public static final String KEY_PARTITION_IDS = "partition_ids";

    // 10 seconds
    public static final long DEFAULT_POLLING_DELAY = 10_000L;
    // 10 second
    public static final long DEFAULT_POLLING_INTERVAL = 10_000L;

    // document & tracked properties
    public static final String KEY_TOPIC_NAME = "topic_name";
    public static final String KEY_MESSAGE_KEY = "message_key";
    public static final String KEY_MESSAGE_OFFSET = "message_offset";
    public static final String KEY_TOPIC_PARTITION = "topic_partition";
    public static final String KEY_MESSAGE_TIMESTAMP = "message_timestamp";
    public static final String HEADER_PROPERTIES_KEY = "header_properties";
    public static final String KEY_PARTITION_ID = "partition_id";
    
    public static final String DYNAMIC_TOPIC_ID = "DYNAMIC_TOPIC";
    public static final String DYNAMIC_TOPIC_LABEL = "Dynamic Topic";

    public static final String TOPIC_NAME_FIELD_ID = "topic_name";
    public static final String TOPIC_NAME_LABEL = "Topic Name";
    public static final String TOPIC_NAME_HELP_TEXT =
            "Enter the topic name for the operation.";

    public static final String CODE_SUCCESS = "SUCCESS";
    public static final String CODE_UNKNOWN_TOPIC = "UNKNOWN_TOPIC_OR_PARTITION";
    public static final String CODE_INVALID_SIZE = "INVALID_SIZE";
    public static final String CODE_TIMEOUT = "TIMEOUT";
    public static final String CODE_ERROR = "ERROR";

    public static final String ERROR_MESSAGE_SIZE_NOT_AVAILABLE = "Message size is not available";

    private Constants() {
    }
}
