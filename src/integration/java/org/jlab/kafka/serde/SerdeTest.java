package org.jlab.kafka.serde;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jlab.kafka.eventsource.EventSourceConfig;
import org.jlab.kafka.eventsource.EventSourceListener;
import org.jlab.kafka.eventsource.EventSourceRecord;
import org.jlab.kafka.eventsource.EventSourceTable;
import org.jlab.kafka.TestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SerdeTest {
    private static Logger LOGGER = LoggerFactory.getLogger(SerdeTest.class);

    private KafkaProducer<TestKey,TestValue> setupTestProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, TestKeySerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TestValueSerializer.class.getName());

        return new KafkaProducer<>(config);
    }

    public void putAll(LinkedHashMap<TestKey, EventSourceRecord<TestKey, TestValue>> database, List<EventSourceRecord<TestKey, TestValue>> records) {
        for(EventSourceRecord<TestKey, TestValue> record: records) {
            database.put(record.getKey(), record);
        }
    }

    public Properties getDefaultConsumerProps(String topicName) {
        Properties props = new Properties();

        props.setProperty(EventSourceConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBootstrapServers());
        props.setProperty(EventSourceConfig.KEY_DESERIALIZER_CLASS_CONFIG, TestKeyDeserializer.class.getName());
        props.setProperty(EventSourceConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TestValueDeserializer.class.getName());
        props.setProperty(EventSourceConfig.TOPIC_CONFIG, topicName);

        return props;
    }

    @Test
    public void basicSerdeTest() throws ExecutionException, InterruptedException, TimeoutException {

        // Admin
        String topicName = TestUtils.setupTopic();


        // Producer
        KafkaProducer<TestKey, TestValue> producer = setupTestProducer();

        TestKey expectedKey1 = new TestKey("hello");
        TestValue expectedValue1 = new TestValue("world");

        producer.send(new ProducerRecord<>(topicName, expectedKey1, expectedValue1)).get();
        producer.send(new ProducerRecord<>(topicName, new TestKey("hey"), new TestValue("oh"))).get();


        // EventSourceTable (Consumer)
        Properties props = getDefaultConsumerProps(topicName);
        final LinkedHashMap<TestKey, EventSourceRecord<TestKey, TestValue>> database = new LinkedHashMap<>();

        try(EventSourceTable<TestKey, TestValue> table = new EventSourceTable<>(props)) {

            table.addListener(new EventSourceListener<TestKey, TestValue>() {
                @Override
                public void batch(List<EventSourceRecord<TestKey, TestValue>> records, boolean highWaterReached) {
                    /*System.out.println("changes: ");
                    for (EventSourceRecord record : records) {
                        System.out.println("Record: " + record);
                    }*/
                    putAll(database, records);
                }
            });

            table.start();

            boolean reached = table.awaitHighWaterOffset(5, TimeUnit.SECONDS);

            if(!reached) {
                throw new TimeoutException("awaitHighWater Timeout!");
            }
        } finally {
            TestUtils.cleanUpTopic(topicName);
        }

        assertEquals(2, database.size());

        EventSourceRecord<TestKey, TestValue> first = database.values().iterator().next();

        assertEquals(expectedKey1, first.getKey());
        assertEquals(expectedValue1, first.getValue());
    }
}
