package org.jlab.kafka.eventsource;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An interface to Apache Kafka that models a topic as a database table.
 * <p>
 * We're using the term Event Sourcing casually to mean persisting state by replaying a stream of saved change events.
 * There are much more complicated, confusing, and nuanced definitions that we're surely stomping all over.
 * </p>
 * <p>
 * This class is similar to KTable in that the most recent record key determines the current record value.  It differs
 * in that it ignores the server persisted client log position (offset) and always rewinds a topic to the beginning
 * (or a specified resume offset) and replays all messages
 * every run and it notifies listeners once
 * the high water mark (highest message index) is reached.
 * </p><p>
 * It's useful for clients which replay events
 * frequently (small-ish data) and are not concerned about scalability, reliability, or intermediate results
 * (transient batch processing of entire final state).
 * </p><p>
 * For clients that cannot tolerate frequent batches and prefers fewer, but bigger batches configure a larger
 * max.poll.records and poll.ms.   For example, a Kafka Connect command topic that triggers a re-balance.
 * </p><p>
 * It is not part of the Kafka Streams API and requires none of
 * that run-time scaffolding.
 * </p>
 * @param <K> The type for message keys
 * @param <V> The type for message values
 */
public class EventSourceTable<K, V> implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(EventSourceTable.class);

    private final KafkaConsumer<K, V> consumer;
    private final EventSourceConfig config;
    private final Set<EventSourceListener<K, V>> listeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private long endOffset = 0;
    private boolean endReached = false;

    private AtomicReference<CONSUMER_STATE> consumerState = new AtomicReference<>(CONSUMER_STATE.INITIALIZING);

    private final CountDownLatch highWaterSignal = new CountDownLatch(1);

    private ExecutorService pollExecutor = null;

    /**
     * Create a new EventSourceTable.
     *
     * @param props The properties - see EventSourceConfig
     */
    public EventSourceTable(Properties props) {

        config = new EventSourceConfig(props);

        // Not sure if there is a better way to get configs (with defaults) from EventSourceConfig into a
        // Properties object (or Map) for KafkaConsumer - we manually copy values over into a new clean Properties.
        // Tried the following without success:
        // - if you use config.valuesWithPrefixOverride() to obtain consumer props it will compile, but serialization
        // may fail at runtime w/ClassCastException! (I guess stuff is mangled from String to Objects or something)
        // - if you simply pass the constructor argument above "props" along to KafkaConsumer, the defaults for missing
        // values won't be set.
        Properties consumerProps = new Properties();

        // Pass values in as is from user (without defaults); then next we'll ensure defaults are used if needed
        // Note: using new Properties(props) does NOT work as that sets the defaults field inside the Properties object,
        // which are not carried over later
        // inside the KafkaConsumer constructor when it also creates a new Properties and uses putAll().
        consumerProps.putAll(props);

        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS));
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_GROUP));
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER));
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getString(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER));

        // Deserializer specific configs are passed in via putAll(props) and don't have defaults in EventSourceConfig
        // Examples:
        // - KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG
        // - KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG


        consumer = new KafkaConsumer<K, V>(consumerProps);
    }

    /**
     * Register an EventSourceListener.
     *
     * @param listener the EventSourceListener
     */
    public void addListener(EventSourceListener<K, V> listener) {
        listeners.add(listener);
    }

    /**
     * Unregister an EventSourceListener.
     *
     * @param listener the EventSourceListener
     */
    public void removeListener(EventSourceListener<K, V> listener) {
        listeners.remove(listener);
    }

    /**
     * Causes the current thread to wait until the high water offset has been reached, unless the thread is
     * interrupted, or the specified waiting time elapses.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if the highWaterOffset was reached before timeout, false otherwise
     * @throws InterruptedException if the current thread is interrupted while waiting
     */
    public boolean awaitHighWaterOffset(long timeout, TimeUnit unit) throws InterruptedException {
        return highWaterSignal.await(timeout, unit);
    }

    /**
     * Start listening for events.
     *
     * @throws IllegalStateException if already running or already closed
     */
    public void start() throws IllegalStateException {
        boolean transitioned = consumerState.compareAndSet(CONSUMER_STATE.INITIALIZING, CONSUMER_STATE.RUNNING);

        if(!transitioned) {
            throw new IllegalStateException("Start has already been called, or the Table has already been closed");
        }

        pollExecutor = Executors.newSingleThreadExecutor();

        pollExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    init();
                    monitorChanges();
                } catch(WakeupException e) {
                    pollExecutor.shutdown();
                    // We expect this when CLOSED (since we call consumer.wakeup()), else throw
                    if(consumerState.get() != CONSUMER_STATE.CLOSED) throw e;
                } finally {
                    consumer.close();
                }
            }
        });
    }

    private void init() {
        log.debug("subscribing to topic: {}", config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC));

        consumer.subscribe(Collections.singletonList(config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC)), new ConsumerRebalanceListener() {

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.debug("Seeking to beginning of topic");

                if(partitions.size() != 1) {
                    throw new IllegalStateException("We only support single partition Event Sourced topics at this time");
                }

                long resumeOffset = config.getLong(EventSourceConfig.EVENT_SOURCE_RESUME_OFFSET);

                TopicPartition p = partitions.iterator().next(); // Exactly one partition verified above

                if(resumeOffset < 0) {
                    consumer.seekToBeginning(partitions);
                } else {
                    consumer.seek(p, resumeOffset);
                }

                Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

                endOffset = endOffsets.get(p);

                if(endOffset == 0 || resumeOffset >= endOffset) {
                    log.debug("No events at resumeOffset or empty topic");
                    endReached = true;
                }
            }
        });

        AtomicBoolean timeout = new AtomicBoolean(false);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        executor.schedule(new Runnable() {
            @Override
            public void run() {
                timeout.set(true);
            }
        }, config.getLong(EventSourceConfig.EVENT_SOURCE_HIGH_WATER_TIMEMOUT),
                TimeUnit.valueOf(config.getString(EventSourceConfig.EVENT_SOURCE_HIGH_WATER_UNITS)));

        boolean provideCompactedCache = config.getBoolean(EventSourceConfig.EVENT_SOURCE_COMPACTED_CACHE);

        List<EventSourceRecord<K, V>> eventRecords = new ArrayList<>();
        LinkedHashMap<K, EventSourceRecord<K,V>> compactedCache = new LinkedHashMap<>();

        while(!endReached && !timeout.get() && consumerState.get() == CONSUMER_STATE.RUNNING) {
            log.debug("polling for changes ({})", config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC));
            ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(config.getLong(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS)));

            if (consumerRecords.count() > 0) { // We have changes
                for(ConsumerRecord<K, V> consumerRecord: consumerRecords) {
                    EventSourceRecord<K, V> eventRecord = consumerToEvent(consumerRecord);
                    eventRecords.add(eventRecord);
                    if(provideCompactedCache) {
                        compactedCache.put(consumerRecord.key(), eventRecord);
                    }

                    if(consumerRecord.offset() + 1 == endOffset) {
                        log.debug("end of partition {} reached", consumerRecord.partition());
                        endReached = true;
                    }
                }

                notifyListenersChanges(eventRecords, false); // Always false while in the init method
            }
        }

        executor.shutdown();

        if(timeout.get()) {
            notifyListenersTimeout();
        } else {
            notifyListenersHighWaterOffset(compactedCache);
        }
    }

    private void monitorChanges() {
        while(consumerState.get() == CONSUMER_STATE.RUNNING) {
            log.debug("polling for changes ({})", config.getString(EventSourceConfig.EVENT_SOURCE_TOPIC));
            ConsumerRecords<K, V> consumerRecords = consumer.poll(Duration.ofMillis(config.getLong(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS)));
            List<EventSourceRecord<K, V>> eventRecords = new ArrayList<>();

            if (consumerRecords.count() > 0) { // We have changes

                for(ConsumerRecord<K, V> consumerRecord: consumerRecords) {
                    EventSourceRecord<K, V> eventRecord = consumerToEvent(consumerRecord);
                    eventRecords.add(eventRecord);
                }

                notifyListenersChanges(eventRecords, true); // Always true while in the monitorChanges method
            }
        }
    }

    private void notifyListenersHighWaterOffset(LinkedHashMap<K, EventSourceRecord<K,V>> compactedCache) {
        for(EventSourceListener<K, V> listener: listeners) {
            listener.highWaterOffset(new LinkedHashMap<>(compactedCache));
        }

        highWaterSignal.countDown();
    }

    private void notifyListenersTimeout() {
        for(EventSourceListener<K, V> listener: listeners) {
            listener.highWaterOffsetTimeout();
        }
    }

    private void notifyListenersChanges(List<EventSourceRecord<K, V>> eventRecords, boolean highWaterReached) {
        for(EventSourceListener<K, V> listener: listeners) {
            listener.batch(new ArrayList<>(eventRecords), highWaterReached);
        }
    }

    private EventSourceRecord<K, V> consumerToEvent(ConsumerRecord<K, V> record) {
        log.debug("Consumer to Event Record: {}={}", record.key(), record.value());
        return new EventSourceRecord<>(record.key(), record.value(), record.offset(), record.timestamp());
    }

    /**
     * Close the underlying Kafka Consumer.
     */
    @Override
    public void close() {
        CONSUMER_STATE previousState = consumerState.getAndSet(CONSUMER_STATE.CLOSED);

        if(previousState == CONSUMER_STATE.INITIALIZING) { // start() never called!
            consumer.close();
        } else {
            consumer.wakeup(); // tap on shoulder and it'll eventually notice consumer state now CLOSED
        }
    }

    private enum CONSUMER_STATE {
        INITIALIZING, RUNNING, CLOSED
    }
}
