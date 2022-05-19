package org.jlab.kafka.eventsource;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EventSourceTest {
    private static Logger LOGGER = LoggerFactory.getLogger(EventSourceTest.class);

    private String getBootstrapServers() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        if(bootstrapServers == null) {
            bootstrapServers = "localhost:9094";
        }

        return bootstrapServers;
    }

    private String setupTopic() throws ExecutionException, InterruptedException, TimeoutException {

        String topicName = UUID.randomUUID().toString();

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {

            Collection<NewTopic> createTopics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));

            adminClient.createTopics(createTopics).all().get(10, TimeUnit.SECONDS);
        }

        return topicName;
    }

    private void cleanUpTopic(String topicName) throws ExecutionException, InterruptedException, TimeoutException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {

            Collection<String> deleteTopics = Collections.singletonList(topicName);

            adminClient.deleteTopics(deleteTopics).all().get(10, TimeUnit.SECONDS);
        }
    }

    private KafkaProducer<String,String> setupProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

         return new KafkaProducer<>(
                config,
                new StringSerializer(),
                new StringSerializer()
        );
    }

    private Properties getDefaultProps(String topicName) {
        Properties props = new Properties();

        props.setProperty(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, getBootstrapServers());
        props.setProperty(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER, StringDeserializer.class.getName());
        props.setProperty(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        props.setProperty(EventSourceConfig.EVENT_SOURCE_TOPIC, topicName);

        return props;
    }

    private void putAll(LinkedHashMap<String, EventSourceRecord<String, String>> database, List<EventSourceRecord<String, String>> records) {
        for(EventSourceRecord<String, String> record: records) {
            database.put(record.getKey(), record);
        }
    }

    @Test
    public void basicTableTest() throws ExecutionException, InterruptedException, TimeoutException {

        // Admin
        String topicName = setupTopic();


        // Producer
        KafkaProducer<String, String> producer = setupProducer();

        producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topicName, "key1", "value2")).get();


        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);
        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    putAll(database, records);
                    System.out.println("changes: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }
                }
            });

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);
        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(1, database.size());
    }

    @Test
    public void resumeOffsetTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = setupTopic();


        // Producer
        KafkaProducer<String, String> producer = setupProducer();

        producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
        producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();


        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_RESUME_OFFSET, "2");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    putAll(database, records);
                    System.out.println("batch: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }
                }
            });

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);
        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(1, database.size());
    }

    @Test
    public void emptyTopicTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = setupTopic();

        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    putAll(database, records);
                    System.out.println("initialState: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }
                }
            });

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);
        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(0, database.size());
    }

    @Test
    public void batchTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = setupTopic();


        // Producer
        KafkaProducer<String, String> producer = setupProducer();

        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {

                    assertEquals(1, records.size());

                    putAll(database, records);
                    System.out.println("initialState: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }
                    calls.getAndIncrement();
                }
            });

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            Thread.sleep(5000);

        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(6, calls.get());
    }

    @Test
    public void cacheTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = setupTopic();


        // Producer
        KafkaProducer<String, String> producer = setupProducer();

        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void highWaterOffset(LinkedHashMap<String, EventSourceRecord<String, String>> records) {
                    System.out.println("initialState: ");
                    database.putAll(records);
                    for (EventSourceRecord record : records.values()) {
                        System.out.println("Record: " + record);
                    }
                    calls.getAndIncrement();
                }
            });

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);

            Thread.sleep(5000);

        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(1, calls.get());
    }

    @Test
    public void cacheDisabledTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = setupTopic();


        // Producer
        KafkaProducer<String, String> producer = setupProducer();

        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_COMPACTED_CACHE, "false");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void highWaterOffset(LinkedHashMap<String, EventSourceRecord<String, String>> records) {
                    System.out.println("initialState: ");
                    database.putAll(records);
                    for (EventSourceRecord record : records.values()) {
                        System.out.println("Record: " + record);
                    }
                    calls.getAndIncrement();
                }
            });

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);

            Thread.sleep(5000);

        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(0, database.size());
        assertEquals(1, calls.get());
    }

    @Test
    public void cacheCompactionTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = setupTopic();


        // Producer
        KafkaProducer<String, String> producer = setupProducer();

        // EventSourceTable (Consumer)
        Properties props = getDefaultProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void highWaterOffset(LinkedHashMap<String, EventSourceRecord<String, String>> records) {
                    System.out.println("initialState: ");
                    database.putAll(records);
                    for (EventSourceRecord record : records.values()) {
                        System.out.println("Record: " + record);
                    }
                    calls.getAndIncrement();
                }
            });

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            producer.send(new ProducerRecord<>(topicName, "key1", null)).get();
            producer.send(new ProducerRecord<>(topicName, "key2", null)).get();
            producer.send(new ProducerRecord<>(topicName, "key3", null)).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            table.start();

            table.awaitHighWaterOffset(5, TimeUnit.SECONDS);

            Thread.sleep(5000);

        } finally {
            cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(1, calls.get());
        assertNull(database.get("key1").getValue());
    }
}
