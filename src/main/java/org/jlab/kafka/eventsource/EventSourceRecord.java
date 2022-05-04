package org.jlab.kafka.eventsource;

import java.util.Objects;

/**
 * A Record in the EventSourceTable.
 *
 * @param <K> The type for message keys
 * @param <V> The type for message values
 */
public class EventSourceRecord<K,V> {
    private K key;
    private V value;
    private long offset;
    private long timestamp;

    /**
     * Create a new EventSourceRecord.
     *
     * @param key the key
     * @param value the value
     * @param offset the offset
     * @param timestamp the timestamp
     */
    public EventSourceRecord(K key, V value, long offset, long timestamp) {
        this.key = key;
        this.value = value;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    /**
     * Return the record key.
     *
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Return the record value.
     *
     * @return the value
     */
    public V getValue() {
        return value;
    }

    /**
     * Return the Kafka log offset.
     *
     * @return the offset
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Return the record timestamp.
     *
     * @return the timestamp
     */
    public long getTimestamp() {
        return timestamp;
    }

    public String toString() {
        return (key == null ? "null" : key.toString()) + "=" + (value == null ? "null" : value.toString());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EventSourceRecord<?, ?> that = (EventSourceRecord<?, ?>) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }
}
