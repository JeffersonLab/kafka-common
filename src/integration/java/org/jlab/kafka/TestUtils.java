package org.jlab.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jlab.kafka.eventsource.EventSourceConfig;
import org.jlab.kafka.eventsource.EventSourceRecord;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TestUtils {
    public static String getBootstrapServers() {
        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");

        if(bootstrapServers == null) {
            bootstrapServers = "localhost:9094";
        }

        return bootstrapServers;
    }

    public static String setupTopic() throws ExecutionException, InterruptedException, TimeoutException {

        String topicName = UUID.randomUUID().toString();

        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {

            Collection<NewTopic> createTopics = Collections.singletonList(new NewTopic(topicName, 1, (short) 1));

            adminClient.createTopics(createTopics).all().get(10, TimeUnit.SECONDS);
        }

        return topicName;
    }

    public static void cleanUpTopic(String topicName) throws ExecutionException, InterruptedException, TimeoutException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(config)) {

            Collection<String> deleteTopics = Collections.singletonList(topicName);

            adminClient.deleteTopics(deleteTopics).all().get(10, TimeUnit.SECONDS);
        }
    }

    public static KafkaProducer<String,String> setupProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());

        return new KafkaProducer<>(
                config,
                new StringSerializer(),
                new StringSerializer()
        );
    }

    public static Properties getDefaultConsumerProps(String topicName) {
        Properties props = new Properties();

        props.setProperty(EventSourceConfig.EVENT_SOURCE_BOOTSTRAP_SERVERS, getBootstrapServers());
        props.setProperty(EventSourceConfig.EVENT_SOURCE_KEY_DESERIALIZER, StringDeserializer.class.getName());
        props.setProperty(EventSourceConfig.EVENT_SOURCE_VALUE_DESERIALIZER, StringDeserializer.class.getName());
        props.setProperty(EventSourceConfig.EVENT_SOURCE_TOPIC, topicName);

        return props;
    }

    public static void putAll(LinkedHashMap<String, EventSourceRecord<String, String>> database, List<EventSourceRecord<String, String>> records) {
        for(EventSourceRecord<String, String> record: records) {
            database.put(record.getKey(), record);
        }
    }
}
