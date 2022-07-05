package org.jlab.kafka.eventsource;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Configuration for an EventSourceTable.  All ConsumerConfig properties are valid, but a few default value overrides
 * are set here.  The EventSourceConfig specific (non ConsumerConfigs) include: TOPIC_CONFIG, HIGH_WATER_TIMEOUT_CONFIG,
 * HIGH_WATER_UNITS_CONFIG, COMPACTED_CACHE_CONFIG, RESUME_OFFSET_CONFIG, and POLL_MS_CONFIG.
 */
public class EventSourceConfig extends AbstractConfig {
    /**
     * event.source.topic - Name of Kafka event source topic to monitor.
     */
    public static final String TOPIC_CONFIG = "event.source.topic";

    /**
     * poll.ms - The amount of time in milliseconds to block waiting for input.
     */
    public static final String POLL_MS_CONFIG = "poll.ms";

    /**
     * high.water.timeout - Timeout for determining high water offset.
     */
    public static final String HIGH_WATER_TIMEOUT_CONFIG = "high.water.timeout";

    /**
     * high.water.units - High water timeout TimeUnit String literal; must match exactly for TimeUnit.valueOf().
     */
    public static final String HIGH_WATER_UNITS_CONFIG = "high.water.units";

    /**
     * compacted.cache - Whether the EventSourceTable should accumulate an in-memory compacted cache
     * (potentially memory intensive).
     */
    public static final String COMPACTED_CACHE_CONFIG = "compacted.cache";

    /**
     * resume.offset - The offset to resume, or -1 to start from the beginning.
     */
    public static final String RESUME_OFFSET_CONFIG = "resume.offset";

    // Use the same identifiers as ConsumerConfig as we'll pass 'em right on through

    /**
     * group.id - Name of Kafka consumer group to use when monitoring the TOPIC.
     */
    public static final String GROUP_ID_CONFIG = "group.id";

    /**
     * bootstrap.servers - Comma-separated list of host and port pairs that are the addresses of the Kafka brokers
     * used to query the TOPIC.
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    /**
     * max.poll.records - The maximum number of records returned in a single call to poll(), and also the maximum
     * batch size returned to the batch call-back.
     */
    public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";

    /**
     * key.deserializer - Class name of deserializer to use for the key.
     */
    public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";

    /**
     * value.deserializer - Class name of deserializer to use for the value.
     */
    public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";

    /**
     * enable.auto.commit - If true the consumer's offset will be periodically committed in the background.
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";

    /**
     * auto.offset.reset - What to do when there is no initial offset in Kafka or if the current offset does not exist
     * any more on the server.
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";

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
                .define(TOPIC_CONFIG,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka event source topic to monitor")
                .define(POLL_MS_CONFIG,
                        ConfigDef.Type.LONG,
                        1000l,
                        ConfigDef.Importance.HIGH,
                        "Milliseconds between polls for topic changes.")
                .define(HIGH_WATER_TIMEOUT_CONFIG,
                        ConfigDef.Type.LONG,
                        5l,
                        ConfigDef.Importance.MEDIUM,
                        "Timeout for determining high water offset.")
                .define(HIGH_WATER_UNITS_CONFIG,
                        ConfigDef.Type.STRING,
                        "SECONDS",
                        ConfigDef.Importance.MEDIUM,
                        "High water timeout TimeUnit String literal; must match exactly for TimeUnit.valueOf().")
                .define(COMPACTED_CACHE_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        "true",
                        ConfigDef.Importance.HIGH,
                        "Whether or not the EventSourceTable should accumulate an in-memory compacted cache (potentially memory intensive).")
                .define(RESUME_OFFSET_CONFIG,
                        ConfigDef.Type.LONG,
                        "-1",
                        ConfigDef.Importance.HIGH,
                        "The offset to resume, or -1 to start from the beginning.")
                .define(GROUP_ID_CONFIG,
                        ConfigDef.Type.STRING,
                        "event-source",
                        ConfigDef.Importance.HIGH,
                        "Name of Kafka consumer group to use when monitoring the TOPIC.")
                .define(BOOTSTRAP_SERVERS_CONFIG,
                        ConfigDef.Type.STRING,
                        "localhost:9092",
                        ConfigDef.Importance.HIGH,
                        "Comma-separated list of host and port pairs that are the addresses of the Kafka brokers used to query the TOPIC.")
                .define(MAX_POLL_RECORDS_CONFIG,
                        ConfigDef.Type.LONG,
                        "500",
                        ConfigDef.Importance.MEDIUM,
                        "The maximum number of records returned in a single call to poll(), and also the maximum batch size returned in the batch call-back.")
                .define(KEY_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the key.")
                .define(VALUE_DESERIALIZER_CLASS_CONFIG,
                        ConfigDef.Type.STRING,
                        "org.apache.kafka.common.serialization.StringDeserializer",
                        ConfigDef.Importance.HIGH,
                        "Class name of deserializer to use for the value.")
                .define(ENABLE_AUTO_COMMIT_CONFIG,
                        ConfigDef.Type.BOOLEAN,
                        false,
                        ConfigDef.Importance.LOW,
                        "If true the consumer's offset will be periodically committed in the background.")
                .define(AUTO_OFFSET_RESET_CONFIG,
                        ConfigDef.Type.STRING,
                        "earliest",
                        ConfigDef.Importance.LOW,
                        "What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server.");

    }
}
