package org.jlab.kafka.eventsource;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EventSourceTest {
    private static Logger LOGGER = LoggerFactory.getLogger(EventSourceTest.class);

    @ClassRule
    public static Network network = Network.newNetwork();

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.1"))
            .withNetwork(network)
            .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix("kafka"))
            .withCreateContainerCmdModifier(cmd -> cmd.withHostName("kafka").withName("kafka"));

    private void setupTopic(String topicName) throws ExecutionException, InterruptedException, TimeoutException {
        AdminClient adminClient = AdminClient.create(ImmutableMap.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ));

        Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));

        adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
    }

    private KafkaProducer<String,String> setupProducer() {
         return new KafkaProducer<>(
                ImmutableMap.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                ),
                new StringSerializer(),
                new StringSerializer()
        );
    }

    private Properties getDefaultProps(String topicName) {
        Properties props = new Properties();

        props.setProperty(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, kafka.getBootstrapServers());
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

        final String topicName = "testing";

        // Admin
        setupTopic(topicName);


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
        }

        assertEquals(1, database.size());
    }

    @Test
    public void resumeOffsetTest() throws ExecutionException, InterruptedException, TimeoutException {

        final String topicName = "testing2";

        // Admin
        setupTopic(topicName);


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
        }

        assertEquals(1, database.size());
    }

    @Test
    public void emptyTopicTest() throws ExecutionException, InterruptedException, TimeoutException {
        final String topicName = "testing3";

        // Admin
        setupTopic(topicName);

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
        }

        assertEquals(0, database.size());
    }

    @Test
    public void batchTest() throws ExecutionException, InterruptedException, TimeoutException {
        final String topicName = "testing4";

        // Admin
        setupTopic(topicName);


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

        }

        assertEquals(6, database.size());
        assertEquals(6, calls.get());
    }

    @Test
    public void cacheTest() throws ExecutionException, InterruptedException, TimeoutException {
        final String topicName = "testing5";

        // Admin
        setupTopic(topicName);


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

        }

        assertEquals(6, database.size());
        assertEquals(1, calls.get());
    }

    @Test
    public void cacheDisabledTest() throws ExecutionException, InterruptedException, TimeoutException {
        final String topicName = "testing6";

        // Admin
        setupTopic(topicName);


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

        }

        assertEquals(0, database.size());
        assertEquals(1, calls.get());
    }

    @Test
    public void cacheCompactionTest() throws ExecutionException, InterruptedException, TimeoutException {
        final String topicName = "testing7";

        // Admin
        setupTopic(topicName);


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

        }

        assertEquals(6, database.size());
        assertEquals(1, calls.get());
        assertNull(database.get("key1").getValue());
    }
}
