package org.jlab.kafka.serde;

import java.util.Objects;

public class TestValue {
    private String field1;

    public TestValue() {

    }
    public TestValue(String field1) {
        this.field1 = field1;
    }

    public String getField1() {
        return field1;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestValue testValue = (TestValue) o;
        return Objects.equals(field1, testValue.field1);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field1);
    }
}
