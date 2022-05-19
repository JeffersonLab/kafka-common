package org.jlab.kafka.serde;

import java.util.Objects;

public class TestKey {
    private String field0;

    public TestKey() {

    }
    public TestKey(String field0) {
        this.field0 = field0;
    }

    public String getField0() {
        return field0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestKey testKey = (TestKey) o;
        return Objects.equals(field0, testKey.field0);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field0);
    }
}
