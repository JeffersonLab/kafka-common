package org.jlab.kafka.serde;

public class TestKeyDeserializer extends JsonDeserializer<TestKey> {
    public TestKeyDeserializer() {
        super(TestKey.class);
    }
}
