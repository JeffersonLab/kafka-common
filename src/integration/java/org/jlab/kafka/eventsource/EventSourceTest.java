package org.jlab.kafka.eventsource;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jlab.kafka.TestUtils;
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

    private static final TIMEOUT_SECONDS = 10;
    
    @Test
    public void basicTableTest() throws ExecutionException, InterruptedException, TimeoutException {

        // Admin
        String topicName = TestUtils.setupTopic();


        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topicName, "key1", "value2")).get();


        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);
        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    /*System.out.println("changes: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }*/
                    TestUtils.putAll(database, records);
                }
            });

            table.start();

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(1, database.size());
    }

    @Test
    public void resumeOffsetTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();


        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
        producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
        producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();


        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_RESUME_OFFSET, "2");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    /*System.out.println("batch: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }*/
                    TestUtils.putAll(database, records);
                }
            });

            table.start();

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(1, database.size());
    }

    @Test
    public void emptyTopicTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();

        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    /*System.out.println("initialState: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }*/
                    TestUtils.putAll(database, records);
                }
            });

            table.start();

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(0, database.size());
    }

    @Test
    public void batchTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();

        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {

                    assertEquals(1, records.size());

                    /*System.out.println("initialState: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }*/
                    TestUtils.putAll(database, records);
                    calls.getAndIncrement();
                }
            });

            table.start();

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            Thread.sleep(5000);

        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(6, calls.get());
    }

    @Test
    public void batchDuplicateTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();

        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void batch(List<EventSourceRecord<String, String>> records, boolean highWaterReached) {
                    //System.out.println("initialState: ");
                    for (EventSourceRecord record : records) {

                        // This test is all about ensuring we don't have duplicates since we never input
                        // duplicates below via producer
                        assertNull(database.get(record.getKey()));

                        //System.out.println("Record: " + record);
                    }
                    TestUtils.putAll(database, records);
                    calls.getAndIncrement();
                }
            });

            producer.send(new ProducerRecord<>(topicName, "key1", "value1")).get();
            producer.send(new ProducerRecord<>(topicName, "key2", "value2")).get();
            producer.send(new ProducerRecord<>(topicName, "key3", "value3")).get();
            producer.send(new ProducerRecord<>(topicName, "key4", "value4")).get();
            producer.send(new ProducerRecord<>(topicName, "key5", "value5")).get();
            producer.send(new ProducerRecord<>(topicName, "key6", "value6")).get();

            Thread.sleep(5000);

            table.start();

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(6, calls.get());
    }

    @Test
    public void cacheTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();

        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void highWaterOffset(LinkedHashMap<String, EventSourceRecord<String, String>> records) {
                    /*System.out.println("initialState: ");
                    for (EventSourceRecord record : records.values()) {
                        System.out.println("Record: " + record);
                    }*/
                    database.putAll(records);
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

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(1, calls.get());
    }

    @Test
    public void cacheDisabledTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();


        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_COMPACTED_CACHE, "false");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void highWaterOffset(LinkedHashMap<String, EventSourceRecord<String, String>> records) {
                    /*System.out.println("initialState: ");
                    for (EventSourceRecord record : records.values()) {
                        System.out.println("Record: " + record);
                    }*/
                    database.putAll(records);
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

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(0, database.size());
        assertEquals(1, calls.get());
    }

    @Test
    public void cacheCompactionTest() throws ExecutionException, InterruptedException, TimeoutException {
        // Admin
        String topicName = TestUtils.setupTopic();


        // Producer
        KafkaProducer<String, String> producer = TestUtils.setupProducer();

        // EventSourceTable (Consumer)
        Properties props = TestUtils.getDefaultConsumerProps(topicName);

        props.setProperty(EventSourceConfig.EVENT_SOURCE_POLL_MILLIS, "100");
        props.setProperty(EventSourceConfig.EVENT_SOURCE_MAX_POLL_RECORDS, "1");

        final LinkedHashMap<String, EventSourceRecord<String, String>> database = new LinkedHashMap<>();

        AtomicInteger calls = new AtomicInteger(0);

        try(EventSourceTable<String, String> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<String, String>() {
                @Override
                public void highWaterOffset(LinkedHashMap<String, EventSourceRecord<String, String>> records) {
                    /*System.out.println("initialState: ");
                    for (EventSourceRecord record : records.values()) {
                        System.out.println("Record: " + record);
                    }*/
                    database.putAll(records);
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

            boolean reached = table.awaitHighWaterOffset(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(6, database.size());
        assertEquals(1, calls.get());
        assertNull(database.get("key1").getValue());
    }
}
