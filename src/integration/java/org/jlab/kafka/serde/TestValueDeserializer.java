package org.jlab.kafka.serde;

public class TestValueDeserializer extends JsonDeserializer<TestValue>  {
    public TestValueDeserializer() {
        super(TestValue.class);
    }
}
