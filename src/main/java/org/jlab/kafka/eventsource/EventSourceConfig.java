package org.jlab.kafka.eventsource;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for an EventSourceTable.
 */
public class EventSourceConfig extends AbstractConfig {
    /**
     * event.source.topic
     */
    public static final String EVENT_SOURCE_TOPIC = "event.source.topic";

    /**
     * poll.ms
     */
    public static final String EVENT_SOURCE_POLL_MILLIS = "poll.ms";

    /**
     * high.water.timeout
     */
    public static final String EVENT_SOURCE_HIGH_WATER_TIMEMOUT = "high.water.timeout";

    /**
     * high.water.units
     */
    public static final String EVENT_SOURCE_HIGH_WATER_UNITS = "high.water.units";

    // Use the same identifiers as ConsumerConfig as we'll pass 'em right on through

    /**
     * group.id
     */
    public static final String EVENT_SOURCE_GROUP = "group.id";

    /**
     * bootstrap.servers
     */
    public static final String EVENT_SOURCE_BOOTSTRAP_SERVERS = "bootstrap.servers";

    /**
     * max.poll.records
     */
    public static final String EVENT_SOURCE_MAX_POLL_RECORDS = "max.poll.records";

    /**
     * key.deserializer
     */
    public static final String EVENT_SOURCE_KEY_DESERIALIZER = "key.deserializer";

    /**
     * value.deserializer
     */
    public static final String EVENT_SOURCE_VALUE_DESERIALIZER = "value.deserializer";

    /**
     * compacted.cache
     */
    public static final String EVENT_SOURCE_COMPACTED_CACHE = "compacted.cache";

    /**
     * resume.offset
     */
    public static final String EVENT_SOURCE_RESUME_OFFSET = "resume.offset";

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
                        "Milliseconds between polls for topic changes - notification delay is EVENT_SOURCE_MAX_BATCH_DELAY times this value with a constant stream of changes, or twice this value when changes are intermittent since the consumer thread waits for 'no changes' poll response before notifying listeners")
                .define(EVENT_SOURCE_HIGH_WATER_TIMEMOUT,
                        ConfigDef.Type.LONG,
                        5l,
                        ConfigDef.Importance.MEDIUM,
                        "Timeout for determining high water offset")
                .define(EVENT_SOURCE_HIGH_WATER_UNITS,
                        ConfigDef.Type.STRING,
                        "SECONDS",
                        ConfigDef.Importance.MEDIUM,
                        "TimeUnit String literal; must match exactly for TimeUnit.valueOf()")
                .define(EVENT_SOURCE_GROUP,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka consumer group to use when monitoring the EVENT_SOURCE_TOPIC")
                .define(EVENT_SOURCE_BOOTSTRAP_SERVERS,
                        ConfigDef.Type.STRING,
                        "localhost:9092",
                        ConfigDef.Importance.HIGH,
                        "Comma-separated list of host and port pairs that are the addresses of the Kafka brokers used to query the EVENT_SOURCE_TOPIC")
                .define(EVENT_SOURCE_MAX_POLL_RECORDS,
                        ConfigDef.Type.LONG,
                        "500",
                        ConfigDef.Importance.MEDIUM,
                        "The maximum number of records returned in a single call to poll(), and also the maximum batch size returned in the batch call-back.")
                .define(EVENT_SOURCE_KEY_DESERIALIZER,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the key")
                .define(EVENT_SOURCE_VALUE_DESERIALIZER,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the value")
                .define(EVENT_SOURCE_COMPACTED_CACHE,
                    ConfigDef.Type.BOOLEAN,
                    "true",
                    ConfigDef.Importance.HIGH,
                    "Whether or not the EventSourceTable should accumulate an in-memory compacted cache (potentially memory intensive)")
                .define(EVENT_SOURCE_RESUME_OFFSET,
                        ConfigDef.Type.LONG,
                        "-1",
                        ConfigDef.Importance.HIGH,
                        "The offset to resume, or -1 to start from the beginning");
    }
}
