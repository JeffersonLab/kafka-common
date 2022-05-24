package org.jlab.kafka.eventsource;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for an EventSourceTable.
 */
public class EventSourceConfig extends AbstractConfig {
    /**
     * event.source.topic - Name of Kafka event source topic to monitor.
     */
    public static final String EVENT_SOURCE_TOPIC = "event.source.topic";

    /**
     * poll.ms - Milliseconds between polls for changes.
     */
    public static final String EVENT_SOURCE_POLL_MILLIS = "poll.ms";

    /**
     * high.water.timeout - Timeout for determining high water offset.
     */
    public static final String EVENT_SOURCE_HIGH_WATER_TIMEMOUT = "high.water.timeout";

    /**
     * high.water.units - High water timeout TimeUnit String literal; must match exactly for TimeUnit.valueOf().
     */
    public static final String EVENT_SOURCE_HIGH_WATER_UNITS = "high.water.units";

    /**
     * compacted.cache - Whether or not the EventSourceTable should accumulate an in-memory compacted cache
     * (potentially memory intensive).
     */
    public static final String EVENT_SOURCE_COMPACTED_CACHE = "compacted.cache";

    /**
     * resume.offset - The offset to resume, or -1 to start from the beginning.
     */
    public static final String EVENT_SOURCE_RESUME_OFFSET = "resume.offset";

    // Use the same identifiers as ConsumerConfig as we'll pass 'em right on through

    /**
     * group.id - Name of Kafka consumer group to use when monitoring the EVENT_SOURCE_TOPIC.
     */
    public static final String EVENT_SOURCE_GROUP = "group.id";

    /**
     * bootstrap.servers - Comma-separated list of host and port pairs that are the addresses of the Kafka brokers
     * used to query the EVENT_SOURCE_TOPIC.
     */
    public static final String EVENT_SOURCE_BOOTSTRAP_SERVERS = "bootstrap.servers";

    /**
     * max.poll.records - The maximum number of records returned in a single call to poll(), and also the maximum
     * batch size returned in the batch call-back.
     */
    public static final String EVENT_SOURCE_MAX_POLL_RECORDS = "max.poll.records";

    /**
     * key.deserializer - Class name of deserializer to use for the key.
     */
    public static final String EVENT_SOURCE_KEY_DESERIALIZER = "key.deserializer";

    /**
     * value.deserializer - Class name of deserializer to use for the value.
     */
    public static final String EVENT_SOURCE_VALUE_DESERIALIZER = "value.deserializer";


    /**
     * Create a new EventSourceConfig with provided originals.
     *
     * @param originals The originals
     */
    public EventSourceConfig(Map originals) {
        super(configDef(), originals, false);
    }

    /**
     * Get the ConfigDef.
     *
     * @return The ConfigDef
     */
    protected static ConfigDef configDef() {
        return new ConfigDef()
                .define(EVENT_SOURCE_TOPIC,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka event source topic to monitor")
                .define(EVENT_SOURCE_POLL_MILLIS,
                        ConfigDef.Type.LONG,
                        1000l,
                        ConfigDef.Importance.HIGH,
                        "Milliseconds between polls for topic changes.")
                .define(EVENT_SOURCE_HIGH_WATER_TIMEMOUT,
                        ConfigDef.Type.LONG,
                        5l,
                        ConfigDef.Importance.MEDIUM,
                        "Timeout for determining high water offset.")
                .define(EVENT_SOURCE_HIGH_WATER_UNITS,
                        ConfigDef.Type.STRING,
                        "SECONDS",
                        ConfigDef.Importance.MEDIUM,
                        "High water timeout TimeUnit String literal; must match exactly for TimeUnit.valueOf().")
                .define(EVENT_SOURCE_COMPACTED_CACHE,
                        ConfigDef.Type.BOOLEAN,
                        "true",
                        ConfigDef.Importance.HIGH,
                        "Whether or not the EventSourceTable should accumulate an in-memory compacted cache (potentially memory intensive).")
                .define(EVENT_SOURCE_RESUME_OFFSET,
                        ConfigDef.Type.LONG,
                        "-1",
                        ConfigDef.Importance.HIGH,
                        "The offset to resume, or -1 to start from the beginning.")
                .define(EVENT_SOURCE_GROUP,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka consumer group to use when monitoring the EVENT_SOURCE_TOPIC.")
                .define(EVENT_SOURCE_BOOTSTRAP_SERVERS,
                        ConfigDef.Type.STRING,
                        "localhost:9092",
                        ConfigDef.Importance.HIGH,
                        "Comma-separated list of host and port pairs that are the addresses of the Kafka brokers used to query the EVENT_SOURCE_TOPIC.")
                .define(EVENT_SOURCE_MAX_POLL_RECORDS,
                        ConfigDef.Type.LONG,
                        "500",
                        ConfigDef.Importance.MEDIUM,
                        "The maximum number of records returned in a single call to poll(), and also the maximum batch size returned in the batch call-back.")
                .define(EVENT_SOURCE_KEY_DESERIALIZER,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the key.")
                .define(EVENT_SOURCE_VALUE_DESERIALIZER,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the value.");
    }
}
