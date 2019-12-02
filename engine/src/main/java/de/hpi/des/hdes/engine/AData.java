package de.hpi.des.hdes.engine;

public class AData<V> {
  private final V value;
  private final long timestamp;

  public AData(final V value, final long timestamp) {
    this.value = value;
    this.timestamp = timestamp;
  }

  public V getValue() {
    return this.value;
  }

  public long getTimestamp() {
    return this.timestamp;
  }
}
