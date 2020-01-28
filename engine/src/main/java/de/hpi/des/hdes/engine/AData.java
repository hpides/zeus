package de.hpi.des.hdes.engine;

import lombok.Getter;

@Getter
public class AData<V> {

  private final V value;
  private final long ingestionTime;
  private final long eventTime;


  private AData(final V value) {
    this.value = value;
    this.ingestionTime = System.nanoTime();
    this.eventTime = 0;
  }

  private AData(final V value, final long ingestionTime, final long eventTime) {
    this.value = value;
    this.ingestionTime = ingestionTime;
    this.eventTime = eventTime;
  }

  public static <V> AData<V> of(V value) {
    return new AData<>(value);
  }

  public <W> AData<W> createNew(W value) {
    return new AData<>(value, this.ingestionTime, this.eventTime);
  }


  @Override
  public String toString() {
    return "AData{" +
        "value=" + value +
        ", ingestionTime=" + ingestionTime +
        ", eventTime=" + eventTime +
        '}';
  }
}
